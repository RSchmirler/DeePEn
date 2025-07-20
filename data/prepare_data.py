import os
import sys
import pandas as pd

# Path to raw DMS files
raw_path = "./data/raw"

# Check if the directory exists
if os.path.exists(raw_path):
    # Get a list of directories in the given path
    subdirectories = [d for d in os.listdir(raw_path) if os.path.isdir(os.path.join(raw_path, d))]
    
    # Check the number of subdirectories
    if len(subdirectories) == 4:
        print(f"The directory '{raw_path}' exists and contains all 4 DMS sets.")
    else:
        print(f"The directory '{raw_path}' exists but contains {len(subdirectories)} subdirectories.")
else:
    print(f"The directory '{raw_path}' does not exist. Please run in /DeePEn ")
    
    
# Path for processed data splits
split_path = "./data/splits"

# Check if the directory exists
if not os.path.exists(split_path):
    # Create the directory since it doesn't exist
    os.makedirs(split_path)
    print(f"Directory '{split_path}' created successfully.")
else:
    print(f"Directory '{split_path}' already exists. Skipping processing raw files")
    sys.exit()
    
    
# Get a list of all DMS datasets
files = os.listdir(raw_path)

for f in files:
    
    raw_path_f =  raw_path + "/" + f
    split_path_f = split_path + "/" + f

    # Create the directory tree
    try:
        os.makedirs(split_path_f)
        print(f"Directory '{split_path_f}' created successfully.")
    except FileExistsError:
        print(f"Directory '{split_path_f}' already exists.")

    
    # Construct splits
    # Define a function to restore the original sequences
    def restore_sequences(df):
        # Extract the wildtype sequence
        wildtype_row = df[df['mutant'] == 'wt']
        wildtype_sequence = wildtype_row['mutated_sequence'].values[0]

        # Function to apply mutations to the wildtype
        def apply_mutations(mutation_str, wildtype_seq):
            sequence = list(wildtype_seq)  # Convert to list for mutability
            for mutation in mutation_str.split(':'):
                original_aa = mutation[0]
                pos = int(mutation[1:-1])
                mutated_aa = mutation[-1]

                # Apply mutation to the wildtype sequence
                sequence[pos - 1] = mutated_aa  # Adjusting for 0-based index
            return ''.join(sequence)

        # Create a new DataFrame with restored sequences
        df_restored = df.copy()
        for index, row in df_restored.iterrows():
            if row['mutant'] != 'wt':
                df_restored.at[index, 'mutated_sequence'] = apply_mutations(row['mutant'], wildtype_sequence)

        return df_restored
    
    # Define the desired datatypes for specific columns
    dtype_dict = {
        "mutant":"object",
        "mutated_sequence":"object",
        "DMS_score":"float64",
        "DMS_score_bin":"float64",
        "mutation_depth":"int64",
        "set":"object",
    }

    combined = pd.read_csv(raw_path_f+"/DMS_raw.csv", dtype=dtype_dict)

    # Use the function to recreate the original DataFrame
    df_restored = restore_sequences(combined)
    
    train_restored = df_restored[df_restored.set == "train"].reset_index(drop=True)
    valid_restored = df_restored[df_restored.set == "valid"].reset_index(drop=True)
    test_restored  = df_restored[df_restored.set == "test"].reset_index(drop=True)

    train_restored["DMS_score_bin"] = train_restored["DMS_score_bin"].astype(int)
    valid_restored["DMS_score_bin"] = valid_restored["DMS_score_bin"].astype(int)
    test_restored["DMS_score_bin"] = test_restored["DMS_score_bin"].astype(int)
    
    train_restored.to_csv(split_path_f + "/" + "train.csv", index=False)
    valid_restored.to_csv(split_path_f + "/" + "valid.csv", index=False)    
    test_restored.to_csv(split_path_f + "/" + "test.csv", index=False)
    
print("Splits created successfully")
