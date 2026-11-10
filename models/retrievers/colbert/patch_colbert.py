"""
This module introduces some fixes for running the colbert package on Windows or MacOS.
"""
import os
import torch
import torch.nn as nn
from typing import Iterable, Set
import colbert.infra.launcher

# Keep a reference to the original functions
original_setup_new_process = colbert.infra.launcher.setup_new_process
original_DDP = nn.parallel.DistributedDataParallel


class PatchedDDP(original_DDP):
    """
        A patched version of DistributedDataParallel that sets device_ids and output_device to None
        when the model is running on CPU, to avoid a ValueError from PyTorch.
    """
    def __init__(self, module, *args, **kwargs):
        is_cpu = all(p.device.type == 'cpu' for p in module.parameters())
        if is_cpu:
            kwargs['device_ids'] = None
            kwargs['output_device'] = None

        super().__init__(module, *args, **kwargs)


def patched_setup_new_process(*args, **kwargs):
    """
        A patched version of colbert.infra.launcher.setup_new_process that:
        1. Sets default environment variables for torch.distributed.
        2. Initializes the process group.
        3. Patches DistributedDataParallel to handle CPU-only training correctly.
        4. Prevents the original setup function from re-initializing the process group.
    """
    # 1. Set environment variables for a single-process run if not set.
    if 'RANK' not in os.environ:
        os.environ['RANK'] = '0'
    if 'WORLD_SIZE' not in os.environ:
        os.environ['WORLD_SIZE'] = '1'
    if 'MASTER_ADDR' not in os.environ:
        os.environ['MASTER_ADDR'] = 'localhost'
    if 'MASTER_PORT' not in os.environ:
        os.environ['MASTER_PORT'] = '12355'

    # 2. Initialize the process group.
    if not torch.distributed.is_initialized():
        torch.distributed.init_process_group(backend='gloo', init_method='env://')

    # 3. Patch DistributedDataParallel for CPU training. Stays in place for the whole run: the
    #    original setup function below runs the entire training loop, so it cannot be restored after.
    nn.parallel.DistributedDataParallel = PatchedDDP

    # 4. Temporarily replace colbert's distributed init to avoid re-initialization.
    import colbert.utils.distributed
    original_colbert_dist_init = colbert.utils.distributed.init

    def mocked_dist_init(*_args, **_kwargs):
        # The original returns (world_size, distributed_module)
        return torch.distributed.get_world_size(), colbert.utils.distributed

    colbert.utils.distributed.init = mocked_dist_init

    # Call the original setup function.
    result = original_setup_new_process(*args, **kwargs)

    # Restore original function
    colbert.utils.distributed.init = original_colbert_dist_init

    return result


CHECKPOINT_STEPS_ENV = 'COLBERT_CHECKPOINT_STEPS'


def _apply_checkpoint_steps(steps: Iterable[int]) -> Set[int]:
    """
        Replace ColBERT's checkpoint milestones in this process.

        The set must be mutated in place: colbert.training.utils binds it with
        `from colbert.parameters import SAVED_CHECKPOINTS`, so rebinding the attribute on
        colbert.parameters would not be visible to it.
    """
    from colbert.training import utils as training_utils

    training_utils.SAVED_CHECKPOINTS.clear()
    training_utils.SAVED_CHECKPOINTS.update(int(step) for step in steps)
    return training_utils.SAVED_CHECKPOINTS


def set_checkpoint_steps(steps: Iterable[int]) -> Set[int]:
    """
        Make ColBERT write a numbered checkpoint at each of the given batch indices.
        colbert.training.utils.manage_checkpoints only saves a `colbert-{batch_idx}` checkpoint when
        batch_idx appears in colbert.parameters.SAVED_CHECKPOINTS, whose milestones start at 10,000
        steps.

        :param steps: batch indices at which to keep a checkpoint
        :return: the live set, after replacement
    """
    steps = sorted({int(step) for step in steps})
    os.environ[CHECKPOINT_STEPS_ENV] = ','.join(str(step) for step in steps)
    return _apply_checkpoint_steps(steps)


# Apply the monkey-patch
colbert.infra.launcher.setup_new_process = patched_setup_new_process

# Re-apply the checkpoint schedule in spawned training processes (see set_checkpoint_steps).
if os.environ.get(CHECKPOINT_STEPS_ENV):
    _apply_checkpoint_steps(int(step) for step in os.environ[CHECKPOINT_STEPS_ENV].split(','))
