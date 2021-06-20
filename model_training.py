from keras.layers import Input, Flatten, Dense
from keras.models import Model
from keras.applications.vgg16 import VGG16
from tensorflow.keras.preprocessing import image_dataset_from_directory
import os
import pandas as pd
import matplotlib.pyplot as plt


BATCH_SIZE = 100
IMG_SIZE = (64, 64)

## LOAD DATA
PATH = f'{os.getcwd()}/images'
train_dir = os.path.join(PATH, 'train')
validation_dir = os.path.join(PATH, 'validation')

df_train = pd.read_pickle(f'{PATH}/train_labels.pkl')
df_val = pd.read_pickle(f'{PATH}/val_labels.pkl')


train_labels = pd.factorize(df_train)[0].tolist()
validation_labels = pd.factorize(df_val)[0].tolist() 

class_names = pd.factorize(df_train)[1].tolist()

print(f'Found {len(train_labels)} training labels and {len(validation_labels)} validation labels')

train_dataset = image_dataset_from_directory(train_dir,
                                            labels=train_labels,
                                            shuffle=True,
                                            batch_size=BATCH_SIZE,
                                            image_size=IMG_SIZE)

validation_dataset = image_dataset_from_directory(validation_dir,
                                                labels=validation_labels,
                                                shuffle=True,
                                                batch_size=BATCH_SIZE,
                                                image_size=IMG_SIZE)

"""
plt.figure(figsize=(10, 10))
for images, labels in train_dataset.take(1):
  for i in range(9):
    ax = plt.subplot(3, 3, i + 1)
    plt.imshow(images[i].numpy().astype("uint8"))
    plt.title(class_names[labels[i]])
    plt.axis("off")
plt.show()
"""

## LOAD MODEL
IMG_SHAPE = IMG_SIZE + (3,)
model = VGG16(input_shape=IMG_SHAPE,weights='imagenet', include_top=False)
model.trainable = False

input_layer = Input(shape=IMG_SHAPE,name = 'image_input')
model = model(input_layer)

x = Flatten(name='flatten')(model)
x = Dense(15, activation='softmax', name='predictions')(x)
model = Model(inputs=input_layer, outputs=x)
model.summary()

## COMPILE MODEL
model.compile(loss='sparse_categorical_crossentropy', optimizer='adam', metrics=['accuracy'])

## TRAIN MODEL
from keras import backend as K
K.clear_session()

initial_epochs = 50
hist = model.fit(train_dataset,
                    epochs=initial_epochs,
                    validation_data=validation_dataset)

## PLOT LEARNING CURVES
acc = hist.history['accuracy']
val_acc = hist.history['val_accuracy']

plt.figure(figsize=(8, 8))

plt.plot(acc, label='Training Accuracy')
plt.plot(val_acc, label='Validation Accuracy')
plt.legend(loc='lower right')
plt.ylabel('Accuracy')
plt.ylim([min(plt.ylim()),1])
plt.title('Training and Validation Accuracy')
plt.show()
