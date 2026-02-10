import numpy as np
import pandas as pd
from scipy import stats

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
from matplotlib.pyplot import cm

from DeePEn_evaluate import calc_metric_depth

# calculates CI for each individual depth bin & data set to only measure variance from different training seeds
def mean_confidence_interval_individual(data, confidence=0.95):
    # Group by 'value' and 'data' columns
    grouped = data.groupby(['depth', 'data'])

    # Calculate mean and standard deviation for each group
    agg_df = grouped['value'].agg(['mean', 'std', 'count']).reset_index()
    
    # Calculate the CI for each group
    agg_df['h'] = agg_df.apply(
    lambda row: row['std'] * stats.t.ppf((1 + confidence) / 2., row['count'] - 1) / np.sqrt(row['count']),
    axis=1)
    
    return agg_df['mean'].mean(), agg_df['h'].mean()

# creates boxplots for chosen models and metrics for chosen DeePEn datasets
def plot_multi_model_multi_metric_DeePEn(methods, datas, metric = ["AUCPR","spearman_func"], main = True, colors = None):
    
    if colors == None:
        colors = ["slategrey",
                  "grey",
                 "darkgreen",
                  "olive",
                 "orangered",
                 "firebrick",
                  "aqua",
                  "royalblue",
                 "goldenrod",
                 "darkorange"]

        # Add four more colors in reserve from tab10
        # Get the `tab10` colormap
        tab10 = plt.get_cmap('tab10')
        # Extract colors from the `tab10` palette
        # Each color is represented as an RGBA tuple, you can convert it to hex if needed
        tab10_colors = [tab10(i) for i in range(tab10.N)]
        # add three more color pairs in reserve
        colors_to_add = [tab10_colors[i] for i in [4,5,6,8]]
        # Convert RGBA to hexadecimal format
        colors_to_add_hex = [mcolors.to_hex(color) for color in colors_to_add]
        # Add these colors to your existing list
        colors.extend(colors_to_add_hex)
    
    colors = colors[:len(methods)]
    
    names = methods
    
    df_metric = []                                                     

    for met in metric:
                                                      
        dfs = []
        dfs_tail = []
                                                      
        for m in methods:

            if main:
                path = "./results/main/" + m
            else:
                path = "./results/SOM/" + m 
            df_concat = pd.DataFrame()
            df_concat_tail = pd.DataFrame()

            for d in datas:

                df = calc_metric_depth(path, d, metric=met)

                seeds = df.iloc[:,5:-1].columns.tolist()

                df_tail = pd.melt(df.tail(max(1,len(df)-6)), id_vars=['depth',  'x'], value_vars=seeds, var_name='Seed', value_name='value')
                df_tail["data"] = d
                df = pd.melt(df.head(min(len(df)-1, 6)).iloc[1:,:], id_vars=['depth',  'x'], value_vars=seeds, var_name='Seed', value_name='value')            
                df["data"] = d

                df_concat = pd.concat([df_concat,df], ignore_index=True)
                df_concat_tail = pd.concat([df_concat_tail,df_tail], ignore_index=True)

            dfs.append(df_concat)
            dfs_tail.append(df_concat_tail)
        df_metric.append((dfs,dfs_tail))
        
    random_baselines = pd.read_csv("./results/main/random_baseline/DeePEn_Table_1_float.csv")

    # Initialize the figure and subplots
    fig, axes = plt.subplots(1, len(metric), figsize=(5 * len(metric) , 6),dpi=300) 
    
    offset = 0.15
    width = 0.2
    overall_linewidth = 0.7
    
    # Customize the mean line properties
    meanprops = {
        'linestyle': '-',
        'linewidth': overall_linewidth,  # Adjust the width according to your preferences
        'color': 'black'
    }
    # Customize the mean line properties
    medianprops = {
        'linestyle': '-',
        'linewidth': overall_linewidth,  # Adjust the width according to your preferences
        'color': 'red',
        'alpha' : 0.5
    }

                                                      
    for k, met in enumerate(metric):
        if len(metric) == 1:
            ax = axes 
        else:
            ax = axes[k]
        
        dfs =  df_metric[k][0]                                             
        dfs_tail =  df_metric[k][1]
        # Initialize list for legend patches
        legend_patches = []
        
        if not  met == "spearman_func":
            ax.axhline(float(random_baselines[random_baselines.method=="random_baseline"][met][0].split(" ")[0]), color='grey', linestyle='--')
            ax.axhline(float(random_baselines[random_baselines.method=="random_baseline"][met][1].split(" ")[0]), color='black', linestyle='--')

        for idx, (df, tail, name, color) in enumerate(zip(dfs, dfs_tail, names, colors)):
            # Create a boxplot for both subplots
            col = 'value'

            bp0 = ax.boxplot(df[col], positions=[idx-offset], widths=width, patch_artist=True, meanline=True, showmeans=True, meanprops=meanprops, medianprops=medianprops)
            bp1 = ax.boxplot(tail[col], positions=[idx+offset], widths=width, patch_artist=True, meanline=True, showmeans=True, meanprops=meanprops, medianprops=medianprops)

            # Calculate mean and confidence intervals for df['mean_all']
            mean_df, ci_df = mean_confidence_interval_individual(df)
            mean_tail, ci_tail = mean_confidence_interval_individual(tail)

            # Overlay error bars with confidence intervals on top of the boxplot
            ax.errorbar(idx-offset, [mean_df], yerr=[ci_df], color='black', capsize=2, capthick=overall_linewidth, elinewidth=overall_linewidth)
            ax.errorbar(idx+offset, [mean_tail], yerr=[ci_tail], color='black', capsize=2, capthick=overall_linewidth, elinewidth=overall_linewidth)

            # Customize the color of each boxplot for both subplots
            color_1 = mcolors.to_rgba(color, alpha=0.25)
            color_2 = mcolors.to_rgba(color, alpha=0.8)

            for element in ['boxes', 'whiskers', 'caps']:
                plt.setp(bp0[element], color=color_1, linewidth=overall_linewidth)
                plt.setp(bp1[element], color=color_2, linewidth=overall_linewidth)

            for patch in bp0['boxes']:
                patch.set(facecolor=color_1)
            for patch in bp1['boxes']:
                patch.set(facecolor=color_2)

            plt.setp(bp0["fliers"], markersize=5, markeredgecolor=color_1, markerfacecolor='none',markeredgewidth=overall_linewidth*0.8)    
            plt.setp(bp1["fliers"], markersize=5, markeredgecolor=color_2, markerfacecolor='none',markeredgewidth=overall_linewidth*0.8)

            # Append a new patch to the legend_patches list for each name and color
            legend_patches.append(mpatches.Patch(color=color, label=name))

        # Set x-ticks and labels for both subplots
        ax.set_ylim(0, 1)
        # Add horizontal grid lines
        ax.grid(axis='y', linestyle='--', alpha=0.7, linewidth=overall_linewidth)

        # Set x-ticks on the upper edge of the second subplot
        ax.set_xticks([])
        
        # Have y-ticks only for the first subplot
        if k > 0:
            ax.set_yticklabels([])            

        ax.set_title(met)
 
    # Create the legend on the plot
    fig.legend(handles=legend_patches, loc='lower center', bbox_to_anchor=(0.5, -0.1), 
          ncol=np.ceil(len(methods) / 4).astype(int), title="Models")
    
    # Display the plot
    plt.show()

# creates results in a df for chosen models and metrics for chosen DeePEn datasets
def table_multi_model_multi_metric_DeePEn(methods, datas, metric = ["AUCPR","spearman_func"], main = True, random_baseline = False, perfect_classifier_baseline = False, aggregate = True, percentage = True, digits = 2):
    
    if random_baseline:
        methods = ["random_baseline"] + methods
    if perfect_classifier_baseline:
        methods = ["perfect_classifier_baseline"] + methods
        
    if percentage:
        percentage_multiplier = 100
    else:
        percentage_multiplier = 1        
    df_metric = []                                                     

    for met in metric:
                                                      
        dfs = []
        dfs_tail = []
                                                      
        for m in methods:

            if main:
                path = "./results/main/" + m
            else:
                path = "./results/SOM/" + m
            df_concat = pd.DataFrame()
            df_concat_tail = pd.DataFrame()

            for d in datas:

                df = calc_metric_depth(path, d, metric=met)

                seeds = df.iloc[:,5:-1].columns.tolist()

                df_tail = pd.melt(df.tail(max(1,len(df)-6)), id_vars=['depth',  'x'], value_vars=seeds, var_name='Seed', value_name='value')
                df_tail["data"] = d
                df = pd.melt(df.head(min(len(df)-1, 6)).iloc[1:,:], id_vars=['depth',  'x'], value_vars=seeds, var_name='Seed', value_name='value')            
                df["data"] = d

                df_concat = pd.concat([df_concat,df], ignore_index=True)
                df_concat_tail = pd.concat([df_concat_tail,df_tail], ignore_index=True)

            dfs.append(df_concat)
            dfs_tail.append(df_concat_tail)
        df_metric.append((dfs,dfs_tail))

    # to analyse results per seed
    if not aggregate:
        return df_metric 

    res = pd.DataFrame(columns=["method","depth"] + metric)  
    
    for k, met in enumerate(metric):

        dfs =  df_metric[k][0]                                             
        dfs_tail =  df_metric[k][1]
        
        for idx, (df, tail, m) in enumerate(zip(dfs, dfs_tail, methods)):

            # Calculate mean and confidence intervals
            mean_df, ci_df = mean_confidence_interval_individual(df)
            if np.isnan(ci_df): ci_df = 0.0
            if percentage:
                new_row = pd.DataFrame([{'method': m, 'depth' : "shallow (5-9)", met : str(int(round(mean_df,digits)*percentage_multiplier)) + " " + "\u00B1" + " " + str(int(round(ci_df,digits)*percentage_multiplier))}])
            else:  
                new_row = pd.DataFrame([{'method': m, 'depth' : "shallow (5-9)", met : str(round(mean_df,digits)*percentage_multiplier) + " " + "\u00B1" + " " + str(round(ci_df,digits)*percentage_multiplier)}])                    
            res = pd.concat([res,new_row], ignore_index=True) 
            
            
            mean_tail, ci_tail = mean_confidence_interval_individual(tail)
            if np.isnan(ci_tail): ci_tail = 0.0
            if percentage:                
                new_row = pd.DataFrame([{'method': m, 'depth' : "higher depth (10+)", met : str(int(round(mean_tail,digits)*percentage_multiplier)) + " " + "\u00B1" + " " + str(int(round(ci_tail,digits)*percentage_multiplier))}])
            else:
                new_row = pd.DataFrame([{'method': m, 'depth' : "higher depth (10+)", met : str(round(mean_tail,digits)*percentage_multiplier) + " " + "\u00B1" + " " + str(round(ci_tail,digits)*percentage_multiplier)}])
            res = pd.concat([res,new_row], ignore_index=True)



    # Group by the identifier columns and apply the first non-null aggregation
    res = res.groupby(['method', 'depth'], as_index=False).first()
    
    # Convert the 'method' column to a categorical type with the specified order
    res['method'] = pd.Categorical(res['method'], categories=methods, ordered=True)

    # Sort the DataFrame by the 'method' column
    res = res.sort_values(by=['method', "depth"], ascending=[True, False]).reset_index(drop=True)
    
    return res           
