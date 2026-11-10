#!/bin/bash
. "$HOME/venvs/locust/bin/activate"

# DEFAULT VARIABLES FOR EXPERIMENTS
export PYTHONPATH=.
export LANGUAGE=nl
export ELASTIC_LOGGING_DISABLED=True

# The model to evaluate. Either the --output_name that was passed to the trainer, or a HuggingFace
# repo ID (e.g. StatisticsNetherlands/GECKOv2-ColBERT-NL), which the retriever downloads and caches.
MODEL_NAME=colbertv2-dutch-all-nodes
# Rounds to evaluate; must match the trainer's --num_rounds. Leave empty for a single checkpoint:
# a HuggingFace model, or one trained with --num_rounds 1, neither of which has round_N directories.
ROUNDS="1 2"
MODE=all                                # must match the trainer's --mode
NBITS=4
K=50
DATASET=data/qa_pairs/nl/complex_nl_test.jsonl
RESULTS_DIR=evaluation/results
BASE_DIR=data/colbert_retriever

# Force a fresh ColBERT index. Needed on the first run after the collection has been rebuilt:
# rebuild_collection() clears experiments/<name>/indexes but not the retriever's own
# experiments/<name>-A|-B directories, so a stale index would otherwise be reused silently.
# Set to False on later runs — indexing the full collection is by far the slowest step here.
OVERRIDE_INDEX=True

# ColBERTRetriever._data_dir() flattens '/' to '_' when turning a checkpoint into a directory name.
# Mirror that here for every path derived from a checkpoint, so HuggingFace repo IDs do not turn
# into nested directories.
flatten() { echo "$1" | tr '/' '_'; }

# ---------------------------------------------------------------------------------------------
# 1. Work out which checkpoints to evaluate.
#
# train_iterative() writes each round to {MODEL_NAME}/round_N, but the retriever flattens the
# checkpoint when locating the collection files. A nested checkpoint therefore resolves its weights
# correctly and then looks for collection_<mode>.tsv under "{MODEL_NAME}_round_N", which does not
# exist. Symlinking that flat name makes both lookups land in the same place. It also gives each
# round its own ColBERT experiment name, so their indexes never collide.
#
# A single checkpoint needs none of this and is passed through exactly as written — a HuggingFace
# repo ID has to reach the retriever with its '/' intact for the download to resolve.
# ---------------------------------------------------------------------------------------------
if [ -z "$ROUNDS" ]; then
    CHECKPOINTS="$MODEL_NAME"
else
    CHECKPOINTS=""
    for round in $ROUNDS; do
        round_dir="$BASE_DIR/$MODEL_NAME/round_$round"
        flat_link="$BASE_DIR/${MODEL_NAME}_round_$round"

        if [ ! -d "$round_dir" ]; then
            echo "ERROR: no such round directory: $round_dir" >&2
            echo "       Check MODEL_NAME and ROUNDS against what the trainer produced, or leave" >&2
            echo "       ROUNDS empty to evaluate $MODEL_NAME itself." >&2
            exit 1
        fi

        if [ -L "$flat_link" ] && [ ! -e "$flat_link" ]; then
            echo "ERROR: $flat_link is a broken symlink. Remove it and re-run." >&2
            exit 1
        fi

        if [ -e "$flat_link" ]; then
            echo "Using existing $flat_link"
        else
            ln -s "$MODEL_NAME/round_$round" "$flat_link"
            echo "Linked $flat_link -> $MODEL_NAME/round_$round"
        fi

        CHECKPOINTS="${CHECKPOINTS:+$CHECKPOINTS }${MODEL_NAME}_round_$round"
    done
fi

# ---------------------------------------------------------------------------------------------
# 2. Check that each checkpoint has a collection to index.
#
# The collection is deliberately decoupled from the weights, so a HuggingFace repo supplies the
# model only: its collection has to be built locally or copied in beforehand. Checking up front
# beats discovering it after the model has been downloaded and loaded.
# ---------------------------------------------------------------------------------------------
for checkpoint in $CHECKPOINTS; do
    data_dir="$BASE_DIR/$(flatten "$checkpoint")"

    if [ ! -f "$data_dir/collection_$MODE.tsv" ]; then
        echo "ERROR: no collection for $checkpoint: $data_dir/collection_$MODE.tsv" >&2
        echo "       Build one against the current graph with:" >&2
        echo "         PYTHONPATH=. python3.13 models/retrievers/colbert/colbert_trainer.py \\" >&2
        echo "             --output_name $(flatten "$checkpoint") --mode $MODE --rebuild_collection" >&2
        exit 1
    fi
done

# ---------------------------------------------------------------------------------------------
# 3. Evaluate each checkpoint.
#
# Everything up to --output_path is parsed by the script's own argparse; --checkpoint and after
# are collected into **model_kwargs and handed to ColBERTRetriever's constructor, which is why
# they do not show up in --help.
# ---------------------------------------------------------------------------------------------
mkdir -p "$RESULTS_DIR"

for checkpoint in $CHECKPOINTS; do
    # The result filename becomes the series label in the comparison plot, so keep it readable.
    output_path="$RESULTS_DIR/$(flatten "$checkpoint").json"

    echo ""
    echo "=== Evaluating $checkpoint ==="
    python3.13 evaluation/evaluate_table_retrieval.py \
        --model_path models/retrievers/colbert/colbert_retriever.py \
        --dataset_path "$DATASET" \
        --query_type sql \
        -k "$K" \
        --output_path "$output_path" \
        --checkpoint "$checkpoint" \
        --mode "$MODE" \
        --nbits "$NBITS" \
        --override_index "$OVERRIDE_INDEX"
done

echo ""
echo "Done. Results written to:"
for checkpoint in $CHECKPOINTS; do
    echo "    $RESULTS_DIR/$(flatten "$checkpoint").json"
done
