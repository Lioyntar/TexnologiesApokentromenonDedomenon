# TexnologiesApokentromenonDedomenon
How to Run:

Install dependencies: pip install -r requirements.txt

Download movies.csv from: https://www.kaggle.com/datasets/mustafasayed1181/movies-metadata-cleaned-dataset-19002025
Ensure movies.csv is in the same folder.

Run the simulation: python run_comparison.py

Notes:

The system uses TCP Sockets (localhost ports 5000+ for Chord, 6000+ for Pastry).

Data is stored persistently in the node_storage folder using B+ Trees.
