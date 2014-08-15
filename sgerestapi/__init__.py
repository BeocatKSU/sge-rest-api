#!/usr/bin/env python
from flask import Flask
app = Flask(__name__)

from sgerestapi.views.beta import api_beta
app.register_blueprint(api_beta, url_prefix='/api/v0.5')
