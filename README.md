YouTube Data Harvesting and Warehousing using SQL and Streamlit

Project Objective:
To create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:
1.  Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
2. Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
3. Option to store the data in a MYSQL or PostgreSQL.
4.Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

Approach:
The first step to extract data is to have Youtube API through which I can extract data from Youtube database.
Creating the Channel, playlist, video, comment functions to extract data from the Youtube channel and storing them in separate lists.
Creating a Mongo database where the collected data from the functions are stored as arrays
Setting up a database in postgresql and creating functions to extract data from Mongo database and save them in database created in postgresql as separate tables for channel, playlist, video, comment.
Finally preparing streamlit codes for performing the above functions when a youtube channel ID is given
A clear data command is also given to delete all data previously stored in Mongodb and Postgresql.
Display the required data when requested and fetch the data as requested in given queries.

Conclusion:
Hence a simple UI with Streamlit, retrieving data from the YouTube API, storing the data SQL as a warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app is built
