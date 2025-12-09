import bisect

class BPlusTreeNode:
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.keys = []
        self.values = []  # Μόνο για leaf nodes
        self.children = [] # Μόνο για internal nodes
        self.next = None  # Pointer για τη λίστα των φύλλων

class BPlusTree:
    def __init__(self, order=5):
        self.root = BPlusTreeNode(leaf=True)
        self.order = order

    def _find_leaf(self, key):
        node = self.root
        while not node.leaf:
            idx = bisect.bisect_right(node.keys, key)
            node = node.children[idx]
        return node

    # --- API (Συμβατό με Dictionary Operations) ---

    def insert(self, key, value):
        leaf = self._find_leaf(key)
        
        # Αν το κλειδί υπάρχει ήδη, κάνουμε update
        if key in leaf.keys:
            idx = leaf.keys.index(key)
            leaf.values[idx] = value
            return

        # Εισαγωγή σε ταξινομημένη θέση
        bisect.insort(leaf.keys, key)
        idx = leaf.keys.index(key)
        leaf.values.insert(idx, value)

        # Split αν γεμίσει ο κόμβος
        if len(leaf.keys) > self.order - 1:
            self._split_leaf(leaf)

    def retrieve(self, key):
        leaf = self._find_leaf(key)
        if key in leaf.keys:
            idx = leaf.keys.index(key)
            return leaf.values[idx]
        return None
    
    def get(self, key): # Alias για συμβατότητα με κώδικα που ζητάει .get()
        return self.retrieve(key)

    def contains(self, key):
        leaf = self._find_leaf(key)
        return key in leaf.keys

    def delete(self, key):
        leaf = self._find_leaf(key)
        if key in leaf.keys:
            idx = leaf.keys.index(key)
            leaf.keys.pop(idx)
            leaf.values.pop(idx)
            return True
        return False

    def items(self):
        """Επιστρέφει λίστα από (key, value) tuples για iteration."""
        current = self.root
        while not current.leaf:
            current = current.children[0]
        
        results = []
        while current:
            for k, v in zip(current.keys, current.values):
                results.append((k, v))
            current = current.next
        return results

    def clear(self):
        self.root = BPlusTreeNode(leaf=True)

    # --- ΒΟΗΘΗΤΙΚΕΣ ΜΕΘΟΔΟΙ ΓΙΑ SPLIT ---
    
    def _split_leaf(self, leaf):
        new_leaf = BPlusTreeNode(leaf=True)
        mid = (len(leaf.keys) + 1) // 2
        
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        
        new_leaf.next = leaf.next
        leaf.next = new_leaf
        
        if leaf == self.root:
            new_root = BPlusTreeNode(leaf=False)
            new_root.keys = [new_leaf.keys[0]]
            new_root.children = [leaf, new_leaf]
            self.root = new_root
        else:
            self._insert_into_parent(leaf, new_leaf.keys[0], new_leaf)

    def _insert_into_parent(self, old_child, key, new_child):
        parent = self._find_parent(self.root, old_child)
        bisect.insort(parent.keys, key)
        idx = parent.keys.index(key)
        parent.children.insert(idx + 1, new_child)
        
        if len(parent.keys) > self.order - 1:
            self._split_internal(parent)

    def _find_parent(self, curr, child):
        if curr.leaf: return None
        for c in curr.children:
            if c == child: return curr
            res = self._find_parent(c, child)
            if res: return res
        return None

    def _split_internal(self, node):
        new_node = BPlusTreeNode(leaf=False)
        mid = len(node.keys) // 2
        split_key = node.keys[mid]
        
        new_node.keys = node.keys[mid+1:]
        new_node.children = node.children[mid+1:]
        
        node.keys = node.keys[:mid]
        node.children = node.children[:mid+1]
        
        if node == self.root:
            new_root = BPlusTreeNode(leaf=False)
            new_root.keys = [split_key]
            new_root.children = [node, new_node]
            self.root = new_root
        else:
            self._insert_into_parent(node, split_key, new_node)