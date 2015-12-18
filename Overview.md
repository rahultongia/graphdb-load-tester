# Background to this project #

While graph databases promise lots of benefits once data is loaded into them I have been frustrated by their ability to load large volumes of data. This project is intended to highlight (and hopefully solve) these issues using a large publicly available data set -the Wikipedia links data.
## Why Wikipedia? ##
The Wikipedia links data is a an example of a very large real-world graph that has a small number of highly connected nodes and many lesser-connected nodes. The data consists of a single file with ~130 million edges of the type articleName1->articleName2 representing links between different Wikipedia pages e.g. "Communism" and "Russia"
## Why is loading this sort of data hard? ##
The challenge in loading data of this sort is that for each edge record added a look-up is required to find the nodes that may or may not already exist in the database. These lookups are done on the key (in this case the Wikipedia article name) and typically require the use of disk-based index that must scale to a very large number of keys. This project includes some example implementations of this key service optimised for this task.