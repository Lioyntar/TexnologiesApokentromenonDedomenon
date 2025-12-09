import hashlib
import csv
import sys
import time

# -----------------------------------------------------------------------------
# ΚΛΑΣΗ NODE: Υλοποίηση του Chord DHT
# -----------------------------------------------------------------------------
class Node:
    def __init__(self, ip, port, m=160):
        self.ip = ip
        self.port = port
        self.m = m
        self.id = self._generate_hash(f"{ip}:{port}")
        self.storage = {} 
        self.finger_table = [None] * m
        
        # Pointers για τον δακτύλιο
        self.successor = self
        self.predecessor = None  # Προσθήκη για ευκολότερο Join/Leave

        # Αρχικοποίηση Finger Table
        for i in range(m):
            self.finger_table[i] = self

    def _generate_hash(self, key):
        key_bytes = key.encode('utf-8')
        sha1 = hashlib.sha1(key_bytes)
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

    # --- API FUNCTIONS (Insert, Lookup, Delete, Update) ---

    def insert_key(self, title, data):
        key = self._generate_hash(title)
        responsible_node = self.find_successor(key)
        responsible_node.storage[key] = data

    def lookup_key(self, title):
        key = self._generate_hash(title)
        print(f"[LOOKUP] '{title}' (Hash: {str(key)[:8]}...)...")
        responsible_node = self.find_successor(key)
        data = responsible_node.storage.get(key)
        if data:
            print(f" -> FOUND at Node {str(responsible_node.id)[:10]}...")
            return data
        else:
            print(f" -> NOT FOUND (Responsible Node {str(responsible_node.id)[:10]}...)")
            return None

    def delete_key(self, title):
        key = self._generate_hash(title)
        responsible_node = self.find_successor(key)
        if key in responsible_node.storage:
            del responsible_node.storage[key]
            print(f"[DELETE] '{title}' deleted from Node {str(responsible_node.id)[:10]}...")
            return True
        return False

    def update_key(self, title, new_data):
        key = self._generate_hash(title)
        responsible_node = self.find_successor(key)
        if key in responsible_node.storage:
            responsible_node.storage[key] = new_data
            print(f"[UPDATE] '{title}' updated in Node {str(responsible_node.id)[:10]}...")
            return True
        return False

    # --- DYNAMIC JOIN & LEAVE OPERATIONS ---

    def join(self, known_node):
        """
        Operation: Node Join
        Ο κόμβος συνδέεται στο δίκτυο μέσω ενός known_node.
        1. Βρίσκει τον successor του.
        2. Ενημερώνει τους pointers (successor/predecessor).
        3. 'Κλέβει' (μεταφέρει) τα κλειδιά που του αντιστοιχούν από τον successor.
        """
        if known_node:
            print(f"[JOIN] Node {str(self.id)[:10]} joining via {str(known_node.id)[:10]}...")
            
            # 1. Βρες τη θέση μου
            self.successor = known_node.find_successor(self.id)
            self.predecessor = self.successor.predecessor
            
            # 2. Ενημέρωση δεικτών των γειτόνων (Double Linked List update)
            # Σημείωση: Σε πραγματικό Chord αυτό γίνεται με stabilization. Εδώ το κάνουμε άμεσα.
            if self.successor.predecessor:
                self.successor.predecessor.successor = self
            self.successor.predecessor = self
            
            # Ενημέρωση Finger Table του εαυτού μου
            self._fix_fingers()
            
            # 3. Μεταφορά Κλειδιών (Key Redistribution)
            # Παίρνω από τον successor όσα κλειδιά είναι <= δικό μου ID
            print(" -> Redistributing keys from successor...")
            keys_to_move = []
            for k, v in self.successor.storage.items():
                # Αν το κλειδί ανήκει πλέον σε εμένα (δηλαδή είναι <= my_id και > my_predecessor_id)
                # Χρησιμοποιούμε την _is_between από τον προηγούμενο στον εαυτό μας
                if self._is_between(k, self.predecessor.id, self.id, inclusive_end=True):
                    keys_to_move.append(k)
            
            for k in keys_to_move:
                self.storage[k] = self.successor.storage[k]
                del self.successor.storage[k]
            
            print(f" -> Took {len(keys_to_move)} keys from Successor.")

    def leave(self):
        """
        Operation: Node Leave
        Ο κόμβος αποχωρεί εθελοντικά.
        1. Μεταφέρει όλα τα κλειδιά του στον successor.
        2. Ενημερώνει τους γείτονες (predecessor.next = successor).
        """
        print(f"[LEAVE] Node {str(self.id)[:10]} is leaving...")
        
        # 1. Μεταφορά δεδομένων στον Successor
        for k, v in self.storage.items():
            self.successor.storage[k] = v
        print(f" -> Transferred {len(self.storage)} keys to successor {str(self.successor.id)[:10]}...")
        self.storage.clear()

        # 2. Ενημέρωση δεικτών (Link patching)
        if self.predecessor:
            self.predecessor.successor = self.successor
        if self.successor:
            self.successor.predecessor = self.predecessor

        # Reset state
        self.successor = self
        self.predecessor = None

    def __repr__(self):
        return f"<Node {str(self.id)[:10]}...>"

# -----------------------------------------------------------------------------
# CSV LOADER
# -----------------------------------------------------------------------------
def load_movies_from_csv(filename, start_node, limit=100):
    print(f"\n--- Loading Data from {filename} (Limit: {limit}) ---")
    try:
        with open(filename, mode='r', encoding='utf-8-sig', errors='replace') as csvfile:
            first_line = csvfile.readline()
            csvfile.seek(0)
            delim = ';' if ';' in first_line else ','
            reader = csv.DictReader(csvfile, delimiter=delim)
            if reader.fieldnames:
                reader.fieldnames = [h.strip().replace('"', '') for h in reader.fieldnames]
            
            count = 0
            for row in reader:
                if limit and count >= limit: break
                t = row.get('title') or row.get('original_title')
                if t:
                    title = t.strip().replace('"', '')
                    try:
                        start_node.insert_key(title, {"title": title, "pop": row.get('popularity')})
                    except: continue
                count += 1
    except FileNotFoundError:
        print("File not found.")

# -----------------------------------------------------------------------------
# MAIN: TEST DYNAMIC JOIN/LEAVE
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # 1. Δημιουργία Αρχικού Δικτύου (Static Wiring για αρχή)
    print("\n--- 1. Initializing Network (10 Nodes) ---")
    nodes = [Node("127.0.0.1", 5000 + i) for i in range(10)]
    nodes.sort(key=lambda x: x.id)
    
    # Χειροκίνητη σύνδεση (predecessors/successors)
    for i in range(len(nodes)):
        nodes[i].successor = nodes[(i + 1) % len(nodes)]
        nodes[i].predecessor = nodes[(i - 1) % len(nodes)]
    
    for node in nodes: node._fix_fingers()

    # 2. Φόρτωση Δεδομένων
    load_movies_from_csv("movies.csv", nodes[0], limit=50)
    
    # 3. TEST NODE JOIN
    print("\n" + "="*40)
    print("TEST: DYNAMIC NODE JOIN")
    print("="*40)
    
    # Δημιουργία νέου κόμβου
    new_node = Node("127.0.0.1", 6000) # New Port
    print(f"New Node created: {str(new_node.id)[:10]}...")
    
    # Επιλογή ενός τυχαίου ταινίας για να δούμε πού βρίσκεται
    test_movie = "Toy Story"
    # Βεβαιωνόμαστε ότι υπάρχει
    nodes[0].insert_key(test_movie, {"title": test_movie}) 
    
    print(f"Before Join: Lookup '{test_movie}'")
    nodes[0].lookup_key(test_movie)
    
    # Ο νέος κόμβος μπαίνει στο δίκτυο (μέσω του nodes[0])
    new_node.join(nodes[0])
    
    # Προσθήκη στη λίστα για να τον έχουμε υπόψη
    nodes.append(new_node)
    nodes.sort(key=lambda x: x.id) # Απλά για να ξέρουμε τη σειρά στο print
    
    print(f"\nAfter Join: Lookup '{test_movie}'")
    # Ψάχνουμε ξανά. Αν ο νέος κόμβος ανέλαβε το κλειδί, θα βρεθεί σε αυτόν.
    nodes[0].lookup_key(test_movie)

    # 4. TEST NODE LEAVE
    print("\n" + "="*40)
    print("TEST: DYNAMIC NODE LEAVE")
    print("="*40)
    
    # Ας πούμε ότι φεύγει ο κόμβος που μόλις μπήκε (ή όποιος έχει το κλειδί)
    key_hash = nodes[0]._generate_hash(test_movie)
    holder = nodes[0].find_successor(key_hash)
    
    print(f"Node {str(holder.id)[:10]} holds '{test_movie}'. Asking it to LEAVE.")
    holder.leave()
    
    # Αφαίρεση από τη λίστα μας (simulation artifact)
    if holder in nodes: nodes.remove(holder)
    
    print(f"\nAfter Leave: Lookup '{test_movie}'")
    # Το κλειδί πρέπει να έχει μεταφερθεί στον successor του και να βρεθεί.
    nodes[0].lookup_key(test_movie)