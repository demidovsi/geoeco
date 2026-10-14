import pymupdf
from PIL import Image
import pytesseract
import matplotlib.pyplot as plt
import numpy as np
import io

# Open the PDF and extract images
doc = pymupdf.open('d16no2024.pdf')
images = []
try:
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        for img_info in page.get_images(full=True):
            xref = img_info[0]
            base_image = doc.extract_image(xref)
            img_bytes = base_image['image']
            images.append(Image.open(io.BytesIO(img_bytes)))
        del page
finally:
    doc.close()

# Assuming there's a specific graph we are interested in, and it's in the first image
# Here you'd need to visually inspect or automate selection of the right image
graph_image = images[0] # Update index based on actual graph position

# Display the image to verify it's the correct graph
plt.imshow(graph_image)
plt.show()

# Here you would implement the process to analyze and extract data points from the graph
# This can be manual or automated depending on the graph complexity and tools available
# Example: Use OCR to read text (not precise for graphs usually)
text = pytesseract.image_to_string(graph_image)
print(text)

# Further processing to extract numerical data would go here