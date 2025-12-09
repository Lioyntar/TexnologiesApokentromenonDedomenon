import hashlib
import csv
import sys

class PastryNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.id_int = self._generate_hash(f"{ip}:{port}")
        self.id_hex = format(self.id_int, '040x') 
        self.storage = {} 
        self.leaf_set = []      
        self.routing_table = [] 

    def _generate_hash(self, key):
        sha1 = hashlib.sha1(key.encode('utf-8'))
        return int(sha1.hexdigest(), 16)

    def match_prefix_len(self, s1, s2):
        length = 0
        min_len = min(len(s1), len(s2))
        for i in range(min_len):
            if s1[i] == s2[i]: length += 1
            else: break
        return length

    def route(self, key_hex):
        if self.id_hex == key_hex: return self, 0
        
        best_node = self
        best_dist = abs(int(self.id_hex, 16) - int(key_hex, 16))

        for node in self.leaf_set:
            dist = abs(int(node.id_hex, 16) - int(key_hex, 16))
            if dist < best_dist:
                best_dist = dist
                best_node = node

        my_match_len = self.match_prefix_len(self.id_hex, key_hex)
        candidate = best_node 
        all_known = self.leaf_set + self.routing_table
        
        for node in all_known:
            node_match_len = self.match_prefix_len(node.id_hex, key_hex)
            if node_match_len > my_match_len: return node, 1 
            if node_match_len == my_match_len:
                dist = abs(int(node.id_hex, 16) - int(key_hex, 16))
                if dist < best_dist:
                    best_dist = dist
                    candidate = node
        return candidate, 1

    def lookup(self, key_hex, hop_count=0):
        next_node, _ = self.route(key_hex)
        if next_node == self:
            return self, hop_count
        return next_node.lookup(key_hex, hop_count + 1)

    # --- API ---
    def insert_key(self, title, data):
        key_int = self._generate_hash(title)
        key_hex = format(key_int, '040x')
        target_node, hops = self.lookup(key_hex)
        target_node.storage[key_hex] = data

    def lookup_key(self, title):
        key_int = self._generate_hash(title)
        key_hex = format(key_int, '040x')
        print(f"[PASTRY LOOKUP] '{title}'...")
        target_node, hops = self.lookup(key_hex)
        data = target_node.storage.get(key_hex)
        if data:
            print(f" -> FOUND at Node {target_node.id_hex[:8]}... in {hops} hops.")
            return data, hops
        else:
            print(f" -> NOT FOUND at Node {target_node.id_hex[:8]}...")
            return None, hops

    def update_key(self, title, data):
        key_int = self._generate_hash(title)
        key_hex = format(key_int, '040x')
        target_node, _ = self.lookup(key_hex)
        if key_hex in target_node.storage:
            target_node.storage[key_hex] = data
            return True
        return False

    def delete_key(self, title):
        key_int = self._generate_hash(title)
        key_hex = format(key_int, '040x')
        target_node, _ = self.lookup(key_hex)
        if key_hex in target_node.storage:
            del target_node.storage[key_hex]
            return True
        return False

    # --- DYNAMIC JOIN & LEAVE ---

    def join(self, known_nodes_list):
        """
        Simulated Pastry Join.
        1. Μαθαίνει για τους άλλους κόμβους (Routing Table Init).
        2. Ενημερώνει τους άλλους κόμβους για την ύπαρξή του.
        3. Παίρνει κλειδιά από γείτονες.
        """
        print(f"[JOIN] Node {self.id_hex[:8]} joining network...")
        
        # 1. Initialize Tables (Copy from network view)
        self.leaf_set = known_nodes_list[:]
        self.routing_table = known_nodes_list[:]
        
        # 2. Update Others (Simulated discovery)
        for node in known_nodes_list:
            if self not in node.leaf_set:
                node.leaf_set.append(self)
                node.routing_table.append(self)
        
        # 3. Key Redistribution (Pull keys closer to me)
        # Ψάχνουμε σε όλους τους κόμβους, αν έχουν κλειδιά που τώρα ανήκουν σε εμένα
        keys_moved = 0
        for node in known_nodes_list:
            keys_to_move = []
            for k, v in node.storage.items():
                # Αν εγώ είμαι πιο κοντά στο κλειδί από τον τωρινό κάτοχο
                dist_me = abs(int(self.id_hex, 16) - int(k, 16))
                dist_owner = abs(int(node.id_hex, 16) - int(k, 16))
                if dist_me < dist_owner:
                    keys_to_move.append(k)
            
            for k in keys_to_move:
                self.storage[k] = node.storage[k]
                del node.storage[k]
                keys_moved += 1
        
        print(f" -> Join Complete. Took over {keys_moved} keys.")

    def leave(self, all_nodes_list):
        """
        Simulated Pastry Leave.
        1. Μεταφέρει δεδομένα στον κοντινότερο γείτονα.
        2. Αφαιρείται από τις λίστες των άλλων.
        """
        print(f"[LEAVE] Node {self.id_hex[:8]} leaving...")
        
        # 1. Transfer keys to nearest neighbor
        if all_nodes_list:
            # Βρες τον κοντινότερο γείτονα που ΔΕΝ είμαι εγώ
            best_neighbor = None
            best_dist = float('inf')
            
            for node in all_nodes_list:
                if node == self: continue
                dist = abs(int(node.id_hex, 16) - int(self.id_int))
                if dist < best_dist:
                    best_dist = dist
                    best_neighbor = node
            
            if best_neighbor:
                print(f" -> Dumping {len(self.storage)} keys to {best_neighbor.id_hex[:8]}...")
                for k, v in self.storage.items():
                    best_neighbor.storage[k] = v
        
        # 2. Update tables of others
        for node in all_nodes_list:
            if self in node.leaf_set: node.leaf_set.remove(self)
            if self in node.routing_table: node.routing_table.remove(self)

        self.storage.clear()


# -----------------------------------------------------------------------------
# MAIN TEST
# -----------------------------------------------------------------------------
def load_movies(filename, node):
    try:
        with open(filename, 'r', encoding='utf-8-sig', errors='replace') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                if count >= 50: break
                t = row.get('title', '').strip().replace('"','')
                if t:
                    node.insert_key(t, {"title":t})
                    count+=1
    except: pass

if __name__ == "__main__":
    nodes = [PastryNode("127.0.0.1", 5000 + i) for i in range(10)]
    # Wiring
    for n in nodes: 
        n.leaf_set = nodes[:]
        n.routing_table = nodes[:]

    load_movies("movies.csv", nodes[0])

    print("\n--- TEST PASTRY JOIN & LEAVE ---")
    test_movie = "Toy Story"
    nodes[0].insert_key(test_movie, {"title": test_movie})
    
    # JOIN
    new_node = PastryNode("127.0.0.1", 7000)
    new_node.join(nodes) # Pass list of existing nodes
    nodes.append(new_node)
    
    nodes[0].lookup_key(test_movie) # Check if still found

    # LEAVE
    holder, _ = nodes[0].lookup(format(nodes[0]._generate_hash(test_movie), '040x'))
    holder.leave(nodes)
    if holder in nodes: nodes.remove(holder)
    
    print("Checking after leave:")
    nodes[0].lookup_key(test_movie)