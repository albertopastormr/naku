from tensorflow.keras.applications.vgg16 import VGG16, preprocess_input, decode_predictions
from tensorflow.keras.preprocessing import image
import numpy as np

filePath = 'images/00000001_000.png'


model = VGG16()


image1 = image.load_img(filePath, target_size = (224, 224))
print(image1)
#The img_to_array() function adds channels: (224, 224, 3) for RGB #and (224, 224, 1) for gray image.
transformedImage = image.img_to_array(image1)
print(transformedImage.shape)
print(transformedImage)

transformedImage = np.expand_dims(transformedImage, axis = 0)
print(transformedImage.shape)

transformedImage = preprocess_input(transformedImage)
print(transformedImage, transformedImage.shape)

prediction = model.predict(transformedImage)
#print(prediction)
print(prediction.shape)

predictionLabel = decode_predictions(prediction, top = 5)
print(predictionLabel)

print('%s (%.2f%%)' % (predictionLabel[0][0][1], predictionLabel[0][0][2]*100 ))
