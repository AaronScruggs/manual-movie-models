import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime


class Movie:

    def __init__(self, movieid, title, genres):
        self.movieid = movieid
        self.title = title
        self.genres = genres

    def return_info(self):
        """
        Used for passing information to save() when needed, so that it
        can be checked against and loaded into the database.
        :return: A dict of movie information.
        """
        info = {"movieid": self.movieid,
                "title": self.title,
                "genres": self.genres
                }
        return info

    @staticmethod
    def create_movie_from_dict(movie_dict):
        """
        :param movie_dict: Values for creating a Movie object.
        :return: Movie object.
        """
        return Movie(movie_dict["movieid"], movie_dict["title"],
                     movie_dict["genres"])

    @staticmethod
    def movie_from_id(cursor, id):
        """
        :param id: A movie id number.
        :return: A Movie object created from a movieid number.
        """
        cursor.execute("SELECT * FROM movies WHERE movieid = %s;", (id,))
        movie_dict = cursor.fetchone()
        return Movie.create_movie_from_dict(movie_dict)

    @staticmethod
    def movie_string_search(cursor, string):
        """
        :param string: String that is matched with movie titles.
        :return: A list Movie objects that contain the search string.
        """
        new_string = "%{}%".format(string)

        cursor.execute("SELECT * FROM movies WHERE lower(title) LIKE %s;",
                       (new_string,)
                       )
        movies = []
        for movied in cursor.fetchall():
            movies.append(Movie.create_movie_from_dict(movied))
        return movies

    @staticmethod
    def movie_by_year(cursor, year):
        """
        :return: A list of Movie objects for all movies in the given year.
        """
        new_string = "%({})%".format(year)

        cursor.execute("SELECT * FROM movies WHERE title LIKE %s;",
                       (new_string,)
                       )
        movies = []
        for movie in cursor.fetchall():
            movies.append(Movie.create_movie_from_dict(movie))
        return movies

    @staticmethod
    def save(cursor, movie):
        """
        :param cursor:
        :param movie: A Move object
        :return: Notify user if the item is already in the datatebase,
        otherwise add it as a new entry.
        """
        info = movie.return_info()
        mid = str(info["movieid"])
        cursor.execute("SELECT * FROM movies WHERE movieid = %s;", (mid,))
        movie_dict = cursor.fetchone()
        if movie_dict is None:
            cursor.execute("INSERT INTO movies (movieid, title, genres)"
                           " VALUES(%s, %s, %s);",
                            (mid, info["title"],
                             info["genres"])
                           )
        else:
            print("Already in the database.")

    def __str__(self):
        return self.title


class Rating:

    def __init__(self, userid, movieid, rating, timestamp, id):
        self.userid = userid
        self.movieid = movieid
        self.rating = rating
        self.timestamp = timestamp
        self.id = id

    @staticmethod
    def avg_filtered_rating(cursor, movieid, review_count):
        """
        :param movieid: The id of the movie to be looked up.
        :param review_count: Minimum number of reviews to return a rating.
        :return: The average rating for a movie if it has enough reviews,
        otherwise None.
        """
        cursor.execute("SELECT avg(r.rating), count(r.rating) FROM ratings r"
                       " WHERE r.movieid = %s",
                       (movieid,)
                       )
        result = list(cursor.fetchone())
        if result[1] > review_count:
            return result[0]
        else:
            return None


class Tag:

    def __init__(self, userid, movieid, tag, timestamp, id):
        self.userid = userid
        self.movieid = movieid
        self.tag = tag

        # Unix timestamp is converted to datetime.
        self.timestamp = datetime.fromtimestamp(int(timestamp)).strftime(
                            ('%Y-%m-%d %H:%M:%S'))
        self.id = id

    @staticmethod
    def get_tags(cursor, movieid):
        """
        :param movieid: Id of a movie in the database.
        :return: A list of tags for that movie.
        """
        cursor.execute("SELECT t.tag FROM tags t WHERE movieid = %s;",
                       (movieid,)
                       )
        result = cursor.fetchall()
        return [x[0] for x in result]

    def __str__(self):
        """ Prints basic tag information along with human readable datetime """
        string = "{} {} {}".format(self.id, self.tag, self.timestamp)
        return string


if __name__ == '__main__':

    conn = psycopg2.connect(host="localhost", database="movies")
    cur = conn.cursor(cursor_factory=DictCursor)

    conn.commit()

