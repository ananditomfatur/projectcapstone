from flask import Flask, request 
import pandas as pd 
import sqlite3
app = Flask(__name__) 

# mendapatkan keseluruhan data dari <data_name>

@app.route('/data/')
def data():
    df = pd.read_csv('data/books_c.csv')
    return (df.to_json())

@app.route('/data/get/<data_name>', methods=['GET']) 
def get_data(data_name): 
    data = pd.read_csv('data/' + str(data_name))
    return (data.to_json())

#data dinamic
@app.route('/data/get/equal/<data_name>/<columns>/<value>', methods=['GET'])
def banyak_bahasa_buku(data_name, columns, value):
    data = pd.read_csv('data/' + str(data_name))
    condition = data[columns] == value
    data = data[condition]
    result = data['language_code'].count()
    return (str(result))

#data static
@app.route('/authors/', methods=['GET'])
def authors():
    df = pd.read_csv('data/books_c.csv')
    df['authors'] = df['authors'].astype('category')
    df['language_code'] = df['language_code'].astype('category')
    df['isbn'] = df['isbn'].astype('category')
    df['isbn13'] = df['isbn13'].astype('category')
    df['bookID'] = df['bookID'].astype('object')
    df_authors = df.groupby(['authors']).mean()
    return df_authors.to_json()

#data static
@app.route('/albums/', methods=['GET'])
def albums():
    conn = sqlite3.connect("data/chinook.db")
    albums_select = pd.read_sql_query(
    '''
    SELECT tracks.*, albums.Title as albumsName,
    artists.Name as ArtistName, genres.Name as GenreName
    FROM tracks
    LEFT JOIN albums 
    ON tracks.AlbumId = albums.AlbumId
    LEFT JOIN artists
    ON albums.ArtistId=artists.ArtistId
    LEFT JOIN genres
    ON genres.genreid=tracks.genreid
    
    ''', conn, index_col='TrackId')
    return albums_select.to_json()

#data static
@app.route('/tesla/', methods=['GET'])
def tesla():
    stock_tesla = pd.read_csv('data/stock_tesla.csv')
    stock_tesla = stock_tesla.stack(level = 0).unstack(level = 1)
    july = pd.date_range(start="2020-07-01", end="2020-07-31")
    stock_tesla = stock_tesla.reindex(july)
    stock_tesla = stock_tesla.ffill()
    return stock_tesla.to_json()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
