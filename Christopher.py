import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Load data from Excel file
df = pd.read_excel('C:/Users/Aditya.Apte/OneDrive - FE fundinfo/Desktop/Test/Email.xlsx')

# Initialize a TfidfVectorizer
tfidf_vectorizer = TfidfVectorizer()

# Generate matrix of word vectors
tfidf_matrix = tfidf_vectorizer.fit_transform(df['Email Body'])

# Compute and print the cosine similarity matrix
cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

# Create a copy to work with
cosine_sim_identical = cosine_sim.copy()

# Set up the condition to identify high cosine similarity (if it isn't exactly comparing with itself)
# Assuming emails with cosine similarity greater than 0.75 as similar, You might want to adjust this depending on your specific needs
cosine_sim_identical[(cosine_sim_identical > 0.90) & (cosine_sim_identical < 0.99)] = 999

# If a pair of emails have cosine similarity of 999, print them out
for i in range(len(cosine_sim_identical)):
    similar_emails = cosine_sim_identical[i] == 999
    if any(similar_emails):
        print(df.iloc[i]['Date'], df.iloc[i]['Agent Name'])
        print('Similar emails:')
        print(df['Email Body'][similar_emails])
        print('------')