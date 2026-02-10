# DeePEn - A Depth sensitive Protein Engineering benchmark
This repository provides a protein fitness prediction benchmark focused on mutational depth.

# ? Preprint coming soon
&nbsp;

<!-- The associated preprint can be found [here](biorxiv link) -->

## Raw data 

Raw data for all four DMS datasets can be found in [/data](data/raw). 

To create the corresponding train / validation / test splits to train your own model please run [prepare_data.py](data/prepare_data.py).

This requires [pandas](https://pandas.pydata.org/docs/getting_started/install.html) to be installed.

```
python data/prepare_data.py
```

## Results 

All results for our study can be found in [/results](results), split into main and SOM (Supporting Online Material)

All display items and analysis relevant for the manuscript can be reproduced by running the [main](notebooks/create_display_items_main.ipynb) and [SOM](notebooks/create_display_items_SOM.ipynb) notebooks.
To run those notebooks, please refer to the required python environment [here](notebooks) 


## Models

Training and inference code and checkpoints for LoRA fine-tuned and evo-tuned ProtT5 models are made available.
To run those notebooks, please refer to the requirements mentioned [here](models)


## Further Analysis

All analysis methods avaialble in this repo are described in detail in this notebook(notebooks/analyse.ipynb)
We also provide a step by step explanation on how to add new models (or DMS datasets)  

##License
   
The data in this repository is released under terms of the [CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/).

The source code in this repository is licensed under the MIT license, which you can find in the MIT-LICENSE.txt file.
