from flask import Flask, jsonify, url_for, render_template, request, redirect, Response, send_file
import flask_monitoringdashboard as dashboard
from sqlitedict import SqliteDict
import random
import string
import sys
import qrcode
import io
import json

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
dict = SqliteDict('./bookmarks.sqlite')

class Bookmark:
    def __init__(self, name, url, description, num_access):
        self.name = name
        self.url = url
        self.description = description
        self.num_access = num_access
    def match_url(self, url):
        return self.url == url

def get_random_alphanumeric_string(length):
    letters_and_digits = string.ascii_letters + string.digits
    return ''.join((random.choice(letters_and_digits) for i in range(length)))

@app.route('/api/bookmarks', methods=['POST', 'GET'])
def post_bookmark():
    error = None
    if request.method == 'POST':
        new_name = request.form['name']
        new_url = request.form['url']
        new_description = request.form['description']
        new_id = get_random_alphanumeric_string(8)
        new_bookmark = Bookmark(new_name, new_url, new_description, 0)
        for key, value in dict.iteritems():
            if value.match_url(new_url):
                return jsonify(reason="The given URL already existed in the system."), 400
        dict[new_id] = new_bookmark
        dict.commit()
        return {"id": f"{new_id}"}, 201

    return render_template('bookmarkindex.html')

@app.route('/api/bookmarks/<bid>', methods=['DELETE', 'GET'])
def show_bookmark(bid):
    if request.method == 'GET':
        bkMark = dict.get(bid)
        if bkMark != None:
            nbkMark = Bookmark(bkMark.name, bkMark.url, bkMark.description, bkMark.num_access+1)
            dict[bid] = nbkMark
            dict.commit()
            del bkMark
            return jsonify(id = bid,name=nbkMark.name,url=nbkMark.url,description=nbkMark.description)
        else:
            return Response(status=404)
    else:
        bkMark = dict.get(bid)
        if bkMark != None:
            del dict[bid]
            return Response(status=204)
        else:
            return Response(status=404)

@app.route('/api/bookmarks/<bid>/qrcode')
def get_qrcode(bid):
    try:
        bkMark = dict.get(bid)
        img = qrcode.make(bkMark.url)
        png_file = io.BytesIO()
        img.save(png_file, 'PNG')
        png_file.seek(0)
        return send_file(png_file, mimetype='image/PNG')
    except:
        return Response(status=404)

@app.route('/api/bookmarks/<bid>/stats')
def get_stats(bid):
    old_eTagV = request.headers.get('ETag')
    bkMark = dict.get(bid)
    if bkMark != None:
        new_eTagV = bkMark.num_access
        if old_eTagV != None:
            if int(new_eTagV) == int(old_eTagV):
                resp = Response(status=304)
            else:
                resp = Response(f'{new_eTagV}', status=200)
        else:
            resp = Response(f'{new_eTagV}', status=200)
        resp.headers['ETag'] = new_eTagV
        return resp
    else:
        return Response(status=404)


if __name__ == '__main__':
    app.run(debug=True)
