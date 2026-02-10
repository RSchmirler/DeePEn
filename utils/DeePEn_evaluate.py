import numpy as np
import pandas as pd
import copy
from scipy import stats
from sklearn.metrics import average_precision_score
from sklearn.metrics import ndcg_score

# gets the cutoff for the last depth bin based on the number of functional variants in your dataset
# the cutoff depth is the last depth bin, every variant deeper than this cutoff will be moved to this bin 
def get_cutoff(data):
    
    # Read the raw CSV file
    df = pd.read_csv("./data/raw/" + data + "/DMS_raw.csv", low_memory=False)
    df = df[df.set!="wt"]
    
    # only functional sequences are taken into account
    df = df[df.DMS_score_bin == 1]
    
    # get cutoff for last depth bucket
    for i in list(np.sort(df.mutation_depth.unique())):
        if len(df[df.mutation_depth>i]) <= 100:
            if len(df[df.mutation_depth>i]) >= 50:
                cutoff = i+1
            else:
                cutoff = i  
            break
            
    return cutoff

# combines the raw df with a result df to have both labels and predictions for downstream analysis
def load_results(data, path):
    
    # Read the raw CSV file
    try:
        df_raw = pd.read_csv("./data/raw/" + data.rsplit('_', 1)[0] + "/DMS_raw.csv", low_memory=False)
    except:
        df_raw = pd.read_csv("./data/raw/" + data + "/DMS_raw.csv", low_memory=False) 
   
    df_raw = df_raw[df_raw.set!="wt"]    
    df_raw = df_raw.drop('mutated_sequence', axis=1)
    df_raw = df_raw.drop('set', axis=1) 
    
    df_raw["DMS_score_bin"] = df_raw["DMS_score_bin"].astype(int)
    
    df_results = pd.read_csv(path + "/" + data + ".csv")
    
    # Merge DataFrames 
    merged_df = pd.merge(df_results, df_raw, on=['mutant'], how = "left")
    
    return merged_df   

# Returns the spearman per depth bin for the depthwise analysis with different training depth
def calc_spearman_depthwise(path, data, training_end_depth):
    
    starting_depth = training_end_depth + 1
    
    # get cutoff for last depth bin
    cutoff = get_cutoff(data)
    
    # load combined dataframe
    df_all = load_results(data + "_" + str(training_end_depth), path)
    # only functional variants
    df = df_all[df_all.DMS_score_bin == 1].copy()

    # get seeds
    seeds = [int(col.split('_', 1)[1]) for col in df.columns if 'seed_' in col]  
    
    # main res df
    res = pd.DataFrame(columns=["depth","x","examples","exmpl_function","frac_function"]+seeds)

    for i in range(starting_depth,cutoff+1):
        new_row = pd.DataFrame([{'depth': i, 'x' : i}])
        res = pd.concat([res,new_row], ignore_index=True)

    for s in seeds:

        for i in range(starting_depth,cutoff):

            res.loc[i-starting_depth,"examples"] = len(df_all[df_all.mutation_depth==i])
            res.loc[i-starting_depth,"exmpl_function"] = len(df[df.mutation_depth==i])
            res.loc[i-starting_depth,"frac_function"] = df_all[df_all.mutation_depth==i].DMS_score_bin.mean()
            res.loc[i-starting_depth,s] = stats.spearmanr(a=df_all[df_all.mutation_depth==i].DMS_score, b=df_all[df_all.mutation_depth==i]["seed_"+str(s)], axis=0)[0]

        # last depth bin  
        res.loc[cutoff-starting_depth,s] = stats.spearmanr(a=df_all[df_all.mutation_depth>=cutoff].DMS_score, b=df_all[df_all.mutation_depth>=cutoff]["seed_"+str(s)], axis=0)[0]  
        res.loc[cutoff-starting_depth,"examples"] = len(df_all[df_all.mutation_depth>=cutoff])
        res.loc[cutoff-starting_depth,"exmpl_function"] = len(df[df.mutation_depth>=cutoff])
        res.loc[cutoff-starting_depth,"frac_function"] = df_all[df_all.mutation_depth>=cutoff].DMS_score_bin.mean()
        res.loc[cutoff-starting_depth,"depth"] = str(cutoff) + "-" + str(df_all.mutation_depth.max())

    # avg over multiple seeds
    res["mean"] = res.iloc[:,-len(seeds):].mean(axis=1)

    return res

# assigns ndcg relevance scores for all functional variants (we min max scale DMS_scores to values between 1 and 3)
def assign_ndcg_scores(df):
    # Initialize the ranking column with 0 (for all non functional sequences)        
    df['ranking'] = 0.0
    # Get the indices where DMS_score_bin is 1
    indices_1 = df.index[df['DMS_score_bin'] == 1]
    # Extract the labels corresponding to these indices
    label_values = df.loc[indices_1, 'DMS_score']
    # Perform min-max scaling to scale values between 1 and 3
    min_label = label_values.min()
    max_label = label_values.max()
    scaled_values = ((label_values - min_label) / (max_label - min_label)) * 2 + 1
    # Assign these scaled values to the `ranking` column in the DataFrame
    df.loc[indices_1, 'ranking'] = scaled_values
    
    return df

# calculates the overlap [%] between the top x predictions and labels
def topx_overlap(col_1, col_2, top_number):
    # Sort each column and get indices of top top_number rows
    top_indices_col1 = col_1.nlargest(top_number).index
    top_indices_col2 = col_2.nlargest(top_number).index

    # Calculate the intersection of indices
    overlap_indices = set(top_indices_col1).intersection(set(top_indices_col2))

    # Calculate the percentage overlap in terms of rows
    percent_overlap = (len(overlap_indices) / top_number)
    
    return percent_overlap

# caluclates the specified metric per depth bin
def calc_metric_depth(path, data, metric="AUCPR"):
    
    # get cutoff for last depth bin
    cutoff = get_cutoff(data)

    # Create baseline results or load result dataframes
    if ("random_baseline" in path) or ("perfect_classifier_baseline" in path):
        # Load the HBI results and strip the result column
        df_all = load_results(data, "./results/main/HBI")
        df_all = df_all.loc[:, ~df_all.columns.str.contains('seed')]
        
        random_cols = [] 
        
        if ("random_baseline" in path):
            # Generate all random columns as a numpy array
            for s in range(1000):
                np.random.seed(s)
                random_cols.append(np.random.rand(len(df_all)))
        else:
            # Generate columns with random values for all functional variants as a numpy array            
            mask = df_all['DMS_score_bin'] == 1
            for s in range(1000):
                np.random.seed(s)
                rand_arr = np.random.rand(len(df_all))
                rand_arr[~mask] = 0  # Set to 0 where DMS_score_bin == 0
                random_cols.append(rand_arr)
        # Create a DataFrame for these columns
        seeds_df = pd.DataFrame(
            np.column_stack(random_cols),
            columns=[f'seed_{s}' for s in range(1000)]
        )

        # Concatenate with df_random
        df_all = pd.concat([df_all, seeds_df], axis=1)

    else:
        # load combined dataframe
        df_all = load_results(data, path)
    
    # Assign relevance score for ndcg calculation
    if "NDCG" in metric:
        df_all =   assign_ndcg_scores(df_all)
        # get the k value from metric
        k_factor = int(metric.split("@")[1]) / 100
    elif "TOP" in metric:
        # get the k value from metric
        k_factor = int(metric.split("_")[1]) / 100        
        
    # only functional variants
    df = df_all[df_all.DMS_score_bin == 1].copy()

    # get seeds
    seeds = [int(col.split('_', 1)[1]) for col in df.columns if 'seed_' in col]         

    #main res df
    res = pd.DataFrame(columns=["depth","x","examples","exmpl_function","frac_function"]+seeds)

    for i in range(4,cutoff+1):
        new_row = pd.DataFrame([{'depth': i, 'x' : i}])
        res = pd.concat([res,new_row], ignore_index=True)

    for s in seeds:

        for i in range(4,cutoff):
            res.loc[i-4,"examples"] = len(df_all[df_all.mutation_depth==i])
            res.loc[i-4,"exmpl_function"] = len(df[df.mutation_depth==i])
            res.loc[i-4,"frac_function"] = df_all[df_all.mutation_depth==i].DMS_score_bin.mean()
            # calculate metric
            if metric == "AUCPR":
                res.loc[i-4,s] = average_precision_score(df_all[df_all.mutation_depth==i].DMS_score_bin, df_all[df_all.mutation_depth==i]["seed_"+str(s)])
            elif metric == "spearman_func":
                res.loc[i-4,s] = stats.spearmanr(a=df[df.mutation_depth==i].DMS_score, b=df[df.mutation_depth==i]["seed_"+str(s)], axis=0)[0]
            elif metric == "spearman_all":
                res.loc[i-4,s] = stats.spearmanr(a=df_all[df_all.mutation_depth==i].DMS_score, b=df_all[df_all.mutation_depth==i]["seed_"+str(s)], axis=0)[0]        
            elif "NDCG" in metric:
                # determine k based on k_factor * number of functional variants in respective bin
                k = int(k_factor * len(df[df.mutation_depth==i]))
                k = max(k,1)
                res.loc[i-4,s] = ndcg_score([list(df_all[df_all.mutation_depth==i].ranking)], 
                                   [list(df_all[df_all.mutation_depth==i]["seed_"+str(s)])], k=k)  
            elif "TOP" in metric:            
                k = int(k_factor * len(df[df.mutation_depth==i]))
                k = max(k,1)
                res.loc[i-4,s] = topx_overlap(df_all[df_all.mutation_depth==i]["DMS_score"], 
                                              df_all[df_all.mutation_depth==i]["seed_"+str(s)], k)

                
        # last depth bin 
        res.loc[cutoff-4,"examples"] = len(df_all[df_all.mutation_depth>=cutoff])
        res.loc[cutoff-4,"exmpl_function"] = len(df[df.mutation_depth>=cutoff])
        res.loc[cutoff-4,"frac_function"] = df_all[df_all.mutation_depth>=cutoff].DMS_score_bin.mean()
        res.loc[cutoff-4,"depth"] = str(cutoff) + "-" + str(df_all.mutation_depth.max())
        # calculate metric
        if metric == "AUCPR":
                res.loc[cutoff-4,s] = average_precision_score(df_all[df_all.mutation_depth>=cutoff].DMS_score_bin, df_all[df_all.mutation_depth>=cutoff]["seed_"+str(s)])  
        elif metric == "spearman_func":
                res.loc[cutoff-4,s] = stats.spearmanr(a=df[df.mutation_depth>=cutoff].DMS_score, b=df[df.mutation_depth>=cutoff]["seed_"+str(s)], axis=0)[0]  
        elif metric == "spearman_all":
                res.loc[cutoff-4,s] = stats.spearmanr(a=df_all[df_all.mutation_depth>=cutoff].DMS_score, b=df_all[df_all.mutation_depth>=cutoff]["seed_"+str(s)], axis=0)[0]                 
        elif "NDCG" in metric:
            # determine k based on k_factor * number of functional variants in respective bin
            k = int(k_factor * len(df[df.mutation_depth>=cutoff]))
            k = max(k,1)
            res.loc[cutoff-4,s] = ndcg_score([list(df_all[df_all.mutation_depth>=cutoff].ranking)], 
                               [list(df_all[df_all.mutation_depth>=cutoff]["seed_"+str(s)])], k=k)
        elif "TOP" in metric:
            k = int(k_factor * len(df[df.mutation_depth>=cutoff]))
            k = max(k,1)
            res.loc[cutoff-4,s] = topx_overlap(df_all[df_all.mutation_depth>=cutoff]["DMS_score"], 
                                              df_all[df_all.mutation_depth>=cutoff]["seed_"+str(s)], k)             

            
    # avg over multiple seeds
    res["mean"] = res.iloc[:,-len(seeds):].mean(axis=1)

    return res     