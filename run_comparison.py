import hashlib
import csv
import random
import os
import threading
import time
import matplotlib.pyplot as plt

# =============================================================================
# 1. CHORD NODE CLASS (Full Implementation)
# =============================================================================
class ChordNode:
    def __init__(self, ip, port, m=160):
        self.ip = ip
        self.port = port
        self.m = m
        self.id = self._generate_hash(f"{ip}:{port}")
        self.storage = {} 
        self.finger_table = [None] * m
        self.successor = self
        self.predecessor = None

        for i in range(m):
            self.finger_table[i] = self

    def _generate_hash(self, key):
        sha1 = hashlib.sha1(key.encode('utf-8'))
        return int(sha1.hexdigest(), 16)

    def _is_between(self, key, n1, n2, inclusive_end=False):
        if n1 < n2:
            return (n1 < key < n2) if not inclusive_end else (n1 < key <= n2)
        else:
            return (n1 < key) or (key < n2) if not inclusive_end else (n1 < key) or (key <= n2)

    def find_successor(self, key):
        if self._is_between(key, self.id, self.successor.id, inclusive_end=True):
            return self.successor
        else:
            n_prime = self.closest_preceding_node(key)
            if n_prime == self:
                return self.successor
            return n_prime.find_successor(key)

    def closest_preceding_node(self, key):
        for i in range(self.m - 1, -1, -1):
            finger = self.finger_table[i]
            if finger and self._is_between(finger.id, self.id, key):
                return finger
        return self

    def _fix_fingers(self):
        for i in range(self.m):
            start = (self.id + 2**i) % (2**self.m)
            self.finger_table[i] = self.find_successor(start)

    def lookup_key(self, title):
        """Thread-safe lookup wrapper for simulation"""
        key = self._generate_hash(title)
        # Simulation delay to make concurrency visible in logs/timing
        time.sleep(random.uniform(0.1, 0.3)) 
        
        node = self.find_successor(key)
        val = node.storage.get(key)
        return val, node.id

    # Dummy methods for consistency if called
    def update_key(self, t, d): pass
    def delete_key(self, t): pass

# =============================================================================
# 2. PASTRY NODE CLASS (Full Implementation)
# =============================================================================
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
        current_node = self
        visited = {self.id_hex} 
        while True:
            next_node, _ = current_node.route(key_hex)
            if next_node == current_node or next_node.id_hex in visited:
                return current_node, hop_count
            current_node = next_node
            visited.add(current_node.id_hex)
            hop_count += 1
            if hop_count > 50: return current_node, hop_count

    def insert_key(self, title, data):
        key_int = self._generate_hash(title)
        key_hex = format(key_int, '040x')
        target_node, hops = self.lookup(key_hex)
        target_node.storage[key_hex] = data

    def lookup_key(self, title):
        """Thread-safe lookup wrapper"""
        key_int = self._generate_hash(title)
        key_hex = format(key_int, '040x')
        # Simulation delay
        time.sleep(random.uniform(0.1, 0.3))
        
        target_node, hops = self.lookup(key_hex)
        return target_node.storage.get(key_hex), hops
        
    # Dummy methods for consistency
    def update_key(self, t, d): pass
    def delete_key(self, t): pass

# =============================================================================
# 3. HELPER FUNCTIONS
# =============================================================================
def load_data_simple(filename, limit):
    titles = []
    print(f"   -> Reading file '{filename}'...")
    try:
        with open(filename, mode='r', encoding='utf-8-sig', errors='replace') as f:
            line = f.readline()
            delim = ';' if ';' in line else ','
            f.seek(0)
            reader = csv.DictReader(f, delimiter=delim)
            if reader.fieldnames:
                reader.fieldnames = [h.strip().replace('"', '') for h in reader.fieldnames]
            count = 0
            for row in reader:
                if count >= limit: break
                t = row.get('title') or row.get('original_title')
                if t:
                    titles.append(t.strip().replace('"', ''))
                    count += 1
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []
    print(f"   -> Loaded {len(titles)} titles.")
    return titles

# --- NEW: CONCURRENCY TEST FUNCTION  ---
def run_concurrent_searches(protocol_name, node_list, queries):
    """
    Runs search queries concurrently using Threads.
    Matches requirement: 'detect concurrently the popularities of the K-movies'
    """
    print(f"\n[CONCURRENCY TEST] Starting {len(queries)} concurrent threads for {protocol_name}...")
    
    threads = []
    results = []

    # Worker function for each thread
    def search_worker(query, idx):
        # print(f"    [Thread-{idx}] START searching for '{query}'")
        start_node = random.choice(node_list)
        
        if protocol_name == "Chord":
            val, node_id = start_node.lookup_key(query)
            # print(f"    [Thread-{idx}] DONE. Found at Node {str(node_id)[:8]}...")
        else:
            val, hops = start_node.lookup_key(query)
            # print(f"    [Thread-{idx}] DONE. Hops: {hops}")
        
        results.append(val)

    # Launch threads
    for i, q in enumerate(queries):
        t = threading.Thread(target=search_worker, args=(q, i))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    print(f"[CONCURRENCY TEST] All {len(queries)} threads finished for {protocol_name}.")

# =============================================================================
# 4. MAIN EXPERIMENT
# =============================================================================
def run_experiment():
    NUM_NODES = 50
    DATA_LIMIT = 200
    FILENAME = "movies.csv"

    print(f"\n--- STARTING FULL PROJECT EXPERIMENT ({NUM_NODES} Nodes) ---")
    
    # 1. Setup Chord
    print("1. Initializing Chord...")
    chord_nodes = [ChordNode("127.0.0.1", 5000 + i) for i in range(NUM_NODES)]
    chord_nodes.sort(key=lambda x: x.id)
    for i in range(NUM_NODES):
        chord_nodes[i].successor = chord_nodes[(i + 1) % NUM_NODES]
    for node in chord_nodes: node._fix_fingers()

    # 2. Setup Pastry
    print("2. Initializing Pastry...")
    pastry_nodes = [PastryNode("127.0.0.1", 6000 + i) for i in range(NUM_NODES)]
    for node in pastry_nodes:
        node.leaf_set = pastry_nodes[:] 
        node.routing_table = pastry_nodes[:]

    # 3. Load Data
    titles = load_data_simple(FILENAME, DATA_LIMIT)
    if not titles: return

    print("   -> Inserting keys...")
    for t in titles:
        # Chord Insert
        key = chord_nodes[0]._generate_hash(t)
        chord_nodes[0].find_successor(key).storage[key] = {"title": t}
        # Pastry Insert
        pastry_nodes[0].insert_key(t, {"title": t})

    # 4. Measure Hops (Sequential)
    print("\n--- PHASE A: Hops Comparison (Sequential) ---")
    test_queries = random.sample(titles, min(20, len(titles)))
    
    chord_total = 0
    pastry_total = 0
    
    for q in test_queries:
        # Chord Hops
        key = chord_nodes[0]._generate_hash(q)
        steps = 0
        curr = chord_nodes[random.randint(0, NUM_NODES-1)]
        while not curr._is_between(key, curr.id, curr.successor.id, inclusive_end=True):
            curr = curr.closest_preceding_node(key)
            steps += 1
            if steps > 50: break
        chord_total += (steps + 1)

        # Pastry Hops
        start = pastry_nodes[random.randint(0, NUM_NODES-1)]
        _, p_hops = start.lookup_key(q)
        pastry_total += p_hops

    avg_chord = chord_total / len(test_queries)
    avg_pastry = pastry_total / len(test_queries)
    
    print(f"\nRESULTS:\nChord Avg Hops: {avg_chord:.2f}\nPastry Avg Hops: {avg_pastry:.2f}")

    # 5. Concurrency Test 
    print("\n--- PHASE B: Concurrency Test (Threads) ---")
    concurrent_queries = random.sample(titles, 5) # Pick 5 random titles to search at once
    print(f"Querying concurrently for: {concurrent_queries}")
    
    # Run Chord Concurrent
    start_time = time.time()
    run_concurrent_searches("Chord", chord_nodes, concurrent_queries)
    print(f"Chord Concurrent Time: {time.time() - start_time:.4f}s")
    
    # Run Pastry Concurrent
    start_time = time.time()
    run_concurrent_searches("Pastry", pastry_nodes, concurrent_queries)
    print(f"Pastry Concurrent Time: {time.time() - start_time:.4f}s")

    # 6. Plot
    print("\n--- PHASE C: Generating Plot ---")
    methods = ['Chord', 'Pastry']
    hops = [avg_chord, avg_pastry]

    try:
        plt.figure(figsize=(8, 6))
        bars = plt.bar(methods, hops, color=['#3498db', '#e67e22'])
        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + 0.1, round(yval, 2), ha='center', va='bottom')
        plt.ylabel('Average Hops')
        plt.title(f'DHT Performance Comparison ({NUM_NODES} Nodes)')
        save_path = os.path.join(os.getcwd(), 'dht_comparison.png')
        plt.savefig(save_path)
        print(f"SUCCESS! Plot saved at: {save_path}")
        plt.show()
    except Exception as e:
        print(f"Error with plotting: {e}")

if __name__ == "__main__":
    run_experiment()
    input("\nPress ENTER to exit...")