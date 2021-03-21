from flask import Flask, jsonify, request
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')

app = Flask(__name__)
sid = SentimentIntensityAnalyzer()

@app.route('/predict', methods=['POST'])
def predict():
    response = request.get_json()
    result = sid.polarity_scores(response['data'])
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
