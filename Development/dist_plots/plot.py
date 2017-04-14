'''
Plot Distribution Data
Author: Yuya Jeremy Ong & Yiyue Zou
'''
import matplotlib.pyplot as plt
import seaborn as sns

# Import Data
product = open('data/product_review_distribution.csv', 'rb').read().split('\n')[:-1]
user = open('data/user_review_distribution.csv', 'rb').read().split('\n')[:-1]

# Preprocess Data
product_values = []
for p in product: product_values += [int(p.split(',')[0])] * int(p.split(',')[1])

user_values = []
for u in user: user_values += [int(u.split(',')[0])] * int(u.split(',')[1])

# Generate XY Data
n, bins, patches = plt.hist(product_values, 1000, normed=1, facecolor='green', alpha=0.75)
plt.title('Review Distribution by Product Frequency')
plt.xlabel('Number of Reviews')
plt.ylabel('Frequency of Products')
plt.axis([0, 500, 0, 0.1])
plt.show()

n, bins, patches = plt.hist(user_values, 2000, normed=1, facecolor='green', alpha=0.75)
plt.title('Review Distribution by User Frequency')
plt.xlabel('Number of Reviews')
plt.ylabel('Frequency of Users')
plt.axis([0, 500, 0, 0.1])
plt.show()
