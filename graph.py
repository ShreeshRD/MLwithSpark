from os import path
#from wordcloud import WordCloud
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter

df = pd.read_csv('sentiment/test.csv')
arr1 = []
arr2 = []
words = []
for tweet in df['Tweet']:
    words.extend(tweet.split(" "))
freq = {}
for word in words:
    if word not in freq:
        freq[word] = 1
    else:
        freq[word] += 1
        
d = Counter(freq)
for k, v in d.most_common(50):
    arr1.append(k)
    arr2.append(v)

plt.plot(arr1,arr2)
plt.xlabel('Iterations')
plt.ylabel('Accuracy')
plt.title(" learning curve")
plt.show()

'''
wordcloud = WordCloud(max_font_size=40).generate(" ".join([(k + ' ') * v for k,v in freq.items()]))
plt.figure()
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()
'''
def acc_graph(filename, title):
    accs = open(filename,"r")
    accs.seek(0)

    lines = accs.readlines()
    X = []
    Y = []

    for line in lines:
        line = line.strip('\n')
        x, y = line.split("   ")
        X.append(int(x))
        Y.append(float(y))
    
    plt.plot(X,Y)
    plt.xlabel('Iterations')
    plt.ylabel('Accuracy')
    plt.title(title+" learning curve")
    plt.show()
    
acc_graph("sgd_accs.txt", "SGD")

acc_graph("PAC_accs.txt", "PAC")
