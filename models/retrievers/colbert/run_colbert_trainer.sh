#!/bin/bash
. "$HOME/venvs/locust/bin/activate"

# DEFAULT VARIABLES FOR EXPERIMENTS
export PYTHONPATH=.
export LANGUAGE=en
export LOCAL_GRAPH=False
export GRAPH_DB_HOST=https://vocabs.cbs.nl/graphdb
export GRAPH_DB_USERNAME=<username>
export GRAPH_DB_PASSWORD=<passwqord>

nohup python3.13 models/retrievers/colbert/colbert_trainer.py \
  --mode all \
  --negative_n 10 \
  --num_rounds 2 \
  --search_k 200 \
  --epochs 10 \
  -b 16 \
  --accumsteps 1 \
  --doc_maxlen 256 \
  -lr 1e-5 \
  --output_name colbertv2-dutch-production \
  > logs/colbert_trainer_dutch.log 2>&1 &
