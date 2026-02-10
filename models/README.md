# Training notebooks for LoRA fine-tuned and evo-tuned ProtT5 models

## Python environment 

**The main packages needed to run those notebooks are:**

- Torch
- Cuda
- Numpy
- Pandas
- Transformers
- Datasets
- biopython

To install everything you need, simply run:

```
pip install -r requirements.txt
```

## Compute requirements

You will need a GPU for model training
- Fine-tuning requires at least a 24GB GPU
- Evo-tuning requires at least a 40GB GPU on the DeepEN proteins
