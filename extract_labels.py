import pandas as pd

BASE_DIR = 'images'
TRAIN_SIZE = 500
VAL_SIZE = 100

df = pd.read_csv(f'{BASE_DIR}/labels.csv')
# sort the order of the images
df= df.sort_values('Image Index')
# get one label per image
df['Finding Labels'] = df['Finding Labels'].apply(lambda x: x.split('|')[0])

# save the labels to the disk
df['Finding Labels'].iloc[ : TRAIN_SIZE].to_pickle(f'{BASE_DIR}/train_labels.pkl')
df['Finding Labels'].iloc[TRAIN_SIZE : TRAIN_SIZE+VAL_SIZE].to_pickle(f'{BASE_DIR}/val_labels.pkl')