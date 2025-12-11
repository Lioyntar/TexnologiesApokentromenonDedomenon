import hashlib
import csv
import random
import os
import threading
import time
import json
import shutil
import matplotlib.pyplot as plt
import numpy as np
import sys

# Έλεγχος για τη βιβλιοθήκη BPlusTree
try:
    from bplustree import BPlusTree
    HAS_BPLUSTREE = True
    print("[SYSTEM] BPlusTree library detected. Using Local Indexing on Disk.")
except ImportError:
    HAS_BPLUSTREE = False
    print("[SYSTEM] WARNING: 'bplustree' not found. Using in-memory dicts.")

# --- IMPORTS ΤΩΝ ΚΛΑΣΕΩΝ ---
try:
    from dht_node import Node as ChordNode
    from pastry_node import PastryNode
except ImportError as e:
    print(f"[ERROR] Could not import node classes: {e}")
    sys.exit(1)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def load_data_full(filename, limit):
    """Φορτώνει δεδομένα από το CSV αρχείο."""
    loaded_items = []
    print(f"   -> Reading file '{filename}'...")
    
    if not os.path.exists(filename):
        print(f"[WARN] File {filename} not found. Generating DUMMY data.")
        return [(f"Movie {i}", {"popularity": str(i*10.5), "year": 2020+i}) for i in range(limit)]
        
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
                    title = t.strip().replace('"', '')
                    loaded_items.append((title, dict(row)))
                    count += 1
    except Exception as e:
        print(f"[ERROR] Reading CSV: {e}")
        return []
    
    print(f"   -> Loaded {len(loaded_items)} movies.")
    return loaded_items

def run_concurrent_searches(node_list, queries):
    """
    Εκτελεί παράλληλες αναζητήσεις και τυπώνει το Popularity score
    για να ικανοποιήσει την απαίτηση της εκφώνησης.
    """
    threads = []
    results = {'hops': [], 'found': 0}
    lock = threading.Lock()
    
    def search_worker(query):
        # Τυχαία επιλογή κόμβου εκκίνησης για την αναζήτηση
        start_node = random.choice(node_list)
        
        # Network Lookup
        val, hops = start_node.lookup_key(query)
        
        with lock:
            results['hops'].append(hops)
            if val: 
                results['found'] += 1
                # --- UPDATE: PRINT POPULARITY ---
                # Τυπώνουμε το αποτέλεσμα για να δείξουμε ότι ανακτήθηκαν τα attributes
                popularity = val.get('popularity', 'N/A')
                print(f"      [SUCCESS] Key: '{query[:20]}...' | Hops: {hops} | Popularity: {popularity}")
            else:
                 pass # print(f"      [FAIL] Key: '{query[:20]}...' not found.")

    for q in queries:
        t = threading.Thread(target=search_worker, args=(q,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
        
    avg_hops = sum(results['hops'])/len(results['hops']) if results['hops'] else 0
    print(f"      Total Found: {results['found']}/{len(queries)} | Avg Hops: {avg_hops:.2f}")
    return avg_hops

# =============================================================================
# MAIN EXPERIMENT
# =============================================================================
def run_experiment():
    NUM_NODES = 30  
    DATA_LIMIT = 100 
    FILENAME = "movies.csv"

    storage_path = os.path.join(os.getcwd(), "node_storage")
    if os.path.exists(storage_path):
        try: shutil.rmtree(storage_path)
        except: pass
    time.sleep(1)

    print(f"\n{'='*60}")
    print(f"STARTING FULL DHT EVALUATION (LOG SCALE VIZ)")
    print(f"{'='*60}")
    
    # ---------------------------------------------------------
    # 1. SETUP NETWORKS
    # ---------------------------------------------------------
    print("\n[1] Initializing Networks (Sockets)...")
    
    # Chord Setup
    print("    -> Setting up Chord ring (Localhost Ports 5000+)...")
    chord_nodes = []
    for i in range(NUM_NODES):
        node = ChordNode("127.0.0.1", 5000 + i)
        chord_nodes.append(node)
    
    # Manual Stabilization for Experiment Stability
    chord_nodes.sort(key=lambda x: x.id)
    for i in range(NUM_NODES):
        chord_nodes[i].successor = chord_nodes[(i + 1) % NUM_NODES].node_info
        chord_nodes[i].predecessor = chord_nodes[(i - 1 + NUM_NODES) % NUM_NODES].node_info
        for k in range(20):
             chord_nodes[i].finger_table[k] = chord_nodes[(i + 2**k) % NUM_NODES].node_info

    # Pastry Setup
    print("    -> Setting up Pastry network (Localhost Ports 6000+)...")
    pastry_nodes = []
    for i in range(NUM_NODES):
        node = PastryNode("127.0.0.1", 6000 + i)
        pastry_nodes.append(node)
    
    # Manual Leaf Set Setup for Experiment Stability
    pastry_nodes.sort(key=lambda x: x.id_int)
    for i in range(NUM_NODES):
        prev_node = pastry_nodes[(i - 1 + NUM_NODES) % NUM_NODES].node_info
        next_node = pastry_nodes[(i + 1) % NUM_NODES].node_info
        pastry_nodes[i].leaf_set = [prev_node, next_node]

    time.sleep(2) 

    items = load_data_full(FILENAME, DATA_LIMIT)
    if not items: return

    times = {'Chord': {}, 'Pastry': {}}
    
    # ---------------------------------------------------------
    # 2. MEASURE INSERT
    # ---------------------------------------------------------
    print("\n[2] Measuring INSERT Performance...")
    
    start = time.time()
    for title, data in items:
        chord_nodes[0].insert_key(title, data)
    times['Chord']['Insert'] = time.time() - start
    print(f"    Chord Insert Time: {times['Chord']['Insert']:.4f}s")
    
    start = time.time()
    for title, data in items:
        pastry_nodes[0].insert_key(title, data)
    times['Pastry']['Insert'] = time.time() - start
    print(f"    Pastry Insert Time: {times['Pastry']['Insert']:.4f}s")

    # ---------------------------------------------------------
    # 3. MEASURE LOOKUP
    # ---------------------------------------------------------
    print("\n[3] Measuring LOOKUP Performance (Concurrent)...")
    try:
        user_input = input("    Enter K (concurrent searches) [default=5]: ")
        K = int(user_input) if user_input.strip() else 5
    except: K = 5
    
    titles_only = [x[0] for x in items]
    queries = random.sample(titles_only, min(K, len(titles_only)))
    print(f"    Searching for {len(queries)} keys...")
    
    print(f"\n    --- Running Chord Lookups (K={K}) ---")
    start = time.time()
    chord_hops = run_concurrent_searches(chord_nodes, queries)
    times['Chord']['Lookup'] = time.time() - start
    
    print(f"\n    --- Running Pastry Lookups (K={K}) ---")
    start = time.time()
    pastry_hops = run_concurrent_searches(pastry_nodes, queries)
    times['Pastry']['Lookup'] = time.time() - start

    # ---------------------------------------------------------
    # 4. MEASURE JOIN
    # ---------------------------------------------------------
    print("\n[4] Measuring DYNAMIC JOIN...")
    
    print("    New Chord Node joining (Port 7000)...")
    new_chord = ChordNode("127.0.0.1", 7000)
    start = time.time()
    new_chord.join(chord_nodes[0])
    times['Chord']['Join'] = time.time() - start
    chord_nodes.append(new_chord)
    print(f"    Chord Join Time: {times['Chord']['Join']:.4f}s")

    print("    New Pastry Node joining (Port 7000)...")
    new_pastry = PastryNode("127.0.0.1", 7000)
    start = time.time()
    new_pastry.join(pastry_nodes[0])
    times['Pastry']['Join'] = time.time() - start
    pastry_nodes.append(new_pastry)
    print(f"    Pastry Join Time: {times['Pastry']['Join']:.4f}s")

    # ---------------------------------------------------------
    # 5. MEASURE LEAVE
    # ---------------------------------------------------------
    print("\n[5] Measuring NODE LEAVE...")
    
    leaving_chord = chord_nodes[5]
    print(f"    Chord Node {leaving_chord.port} leaving...")
    start = time.time()
    leaving_chord.leave()
    times['Chord']['Leave'] = time.time() - start
    if leaving_chord in chord_nodes: chord_nodes.remove(leaving_chord)
    print(f"    Chord Leave Time: {times['Chord']['Leave']:.4f}s")

    leaving_pastry = pastry_nodes[5]
    print(f"    Pastry Node {leaving_pastry.port} leaving...")
    start = time.time()
    leaving_pastry.leave()
    times['Pastry']['Leave'] = time.time() - start
    if leaving_pastry in pastry_nodes: pastry_nodes.remove(leaving_pastry)
    print(f"    Pastry Leave Time: {times['Pastry']['Leave']:.4f}s")

    # ---------------------------------------------------------
    # 6. UPDATE & DELETE
    # ---------------------------------------------------------
    print("\n[6] Measuring UPDATE...")
    upd_key = queries[0]
    
    start = time.time()
    # Ενημέρωση με διατήρηση του popularity για να είναι ρεαλιστικό
    chord_nodes[0].update_key(upd_key, {"popularity": "99.9 (Updated)", "status": "updated"})
    times['Chord']['Update'] = time.time() - start
    print(f"    Chord Update Time: {times['Chord']['Update']:.4f}s")
    
    start = time.time()
    pastry_nodes[0].update_key(upd_key, {"popularity": "99.9 (Updated)", "status": "updated"})
    times['Pastry']['Update'] = time.time() - start
    print(f"    Pastry Update Time: {times['Pastry']['Update']:.4f}s")

    print("\n[7] Measuring DELETE...")
    start = time.time()
    chord_nodes[0].delete_key(upd_key)
    times['Chord']['Delete'] = time.time() - start
    print(f"    Chord Delete Time: {times['Chord']['Delete']:.4f}s")

    start = time.time()
    pastry_nodes[0].delete_key(upd_key)
    times['Pastry']['Delete'] = time.time() - start
    print(f"    Pastry Delete Time: {times['Pastry']['Delete']:.4f}s")

    # ---------------------------------------------------------
    # PLOTTING WITH LOG SCALE
    # ---------------------------------------------------------
    print("\n--- Generating Plots (Logarithmic) ---")
    operations = ['Insert', 'Lookup', 'Join', 'Leave', 'Update', 'Delete']
    
    # Ensure no zero values for Log Plot (minimum 0.0001s)
    chord_vals = [max(times['Chord'].get(op, 0), 0.0001) for op in operations]
    pastry_vals = [max(times['Pastry'].get(op, 0), 0.0001) for op in operations]

    x = np.arange(len(operations))
    width = 0.35

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # --- Plot 1: Logarithmic Time ---
    rects1 = ax1.bar(x - width/2, chord_vals, width, label='Chord', color='blue')
    rects2 = ax1.bar(x + width/2, pastry_vals, width, label='Pastry', color='orange')
    
    ax1.set_yscale('log') # Ενεργοποίηση Λογαριθμικής Κλίμακας
    ax1.set_ylabel('Time (Seconds) - Log Scale')
    ax1.set_title('Socket-based Performance (Log Scale)')
    ax1.set_xticks(x)
    ax1.set_xticklabels(operations)
    ax1.legend()
    ax1.grid(True, which="both", ls="--", linewidth=0.5, alpha=0.5)

    # --- Plot 2: Hops (Linear) ---
    ax2.bar(['Chord', 'Pastry'], [chord_hops, pastry_hops], color=['green', 'red'])
    ax2.set_ylabel('Average Hops')
    ax2.set_title(f'Lookup Hops (K={K})')

    plt.tight_layout()
    save_path = os.path.join(os.getcwd(), 'dht_full_comparison.png')
    plt.savefig(save_path)
    print(f"Plots saved as '{save_path}'")

    print("Stopping Servers...")
    try: new_chord.cleanup()
    except: pass
    try: new_pastry.cleanup()
    except: pass
    
    for n in chord_nodes: n.cleanup()
    for n in pastry_nodes: n.cleanup()
    sys.exit(0)

if __name__ == "__main__":
    run_experiment()