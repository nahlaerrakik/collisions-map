__author__ = 'nahla.errakik'

import main
from flask import Flask, render_template


app = Flask(__name__)


@app.route("/")
def home():
    return render_template('index.html')


if __name__ == "__main__":
    main.main()
    app.run(debug=False)
