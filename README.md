# Documentation
I have added explanation of code wherever it is needed.

### Loaders
The Loaders for both movies and ratings are pretty straightforward.
I have used a simple split on the "|" character in both cases. I have had to take care of some edge cases, like a movie having only one genre.

## Milestone 1

### Task 2.1
For grouping ratings by year I had to convert UNIX Epoch time to year. I did this using the system defined DateTime() function. Inside each of the yearwise groups, I created another group based on the IDs of the movie as expected from the question.

### Task 2.2
Straightforward implementation.

### Task 2.3 and 2.4
In these tasks, for every year I have to find the most popular movie or its genres. Since this operation is for every year, and since the number of years are only few, I decided to go ahead with a regular for loop. I discussed with one of the TAs to confirm that it should not cause efficiency issues, and indeed it does not. For every year, I look at the number of movies rated (it is an easy operation since we have grouped movies by year and then by movieID). If any movies has a greater number of ratings, I store it as the most popular movie for that year. If it has the same number as some other year, I use a tie-breaking rule based on the ID value. Finally, I perform a join based on the ID of the movie to get the title or genres.

### Task 2.5
Since I want the global maximum and minimum of all time for every genre, I collect all the RDDs in one place. Then I make key-value pairs for each genre, the key is the genre and the value is the number of movies rated all time. I sort this dictionary to obtain the required minimum and maximum.

### Task 2.6
Again, we need to collect because we want all the movies for a given set of genres. I use the filter function to only keep those movies that have all the genres from the required list.

## Task 2.7
This is the same task as before, but I now broadcast my list of required genres. Again, I use the filter function to only keep those movies that have all the genres from the required list.

## Milestone 2

For this Milestone, I have to be able to present the user with the average rating for a given movie at any given time. One can also add or update ratings for a movie at any given time. Hence, this data structure must be able to efficiently do all of this.
Hence, I have made a data structure that, for every movie ID, stores the sum of all the ratings seen so far for that movie along with the total number of ratings. For every new rating that is added, we add the rating to the sum, and increment the total number of ratings seen so far by one. Then, to present a rating for a movie, we simple go to the movie ID and return the sum of ratings divided by the number of ratings for that movie. To update the rating of a user, we subtract the previous rating and add the new rating at that entry and don't change the number of ratings.

## Milestone 3

In this Milestone, I have mostly followed the instructions as described in the project description.
I give a signature to a title based on its genres and then cluster the same signatures together into buckets. I have NNLookup and LSHIndex in a very straightforward way.

### Basline Predictor
In the baseline predictor, one important consideration for me was the optimisation. A previous version of the code where I only made RDDs in the init part, and left the collection for later caused my code to be very slow. This was because it had to collect all the RDDs for every predict. 
A better way to do this was to do one collect at the end of the init. This was, I would just have to call the two maps and do some basic arithmetic with the numbers obtained, which was very inexpensive in comparison.

The rest of the implementation is straightforward and has been documented in the code itself.

