# Draw Together!
Draw Together! is a course project in Distributed System class. 
## What is Draw Together!
DrawTogether! is an online collaborative drawing game. Multiple players can accomplish a picture together. A player can see others’ changes in real time. Just enjoy it! :)
## What can Draw Together! do
Players can use the pencil to draw on the canvas. You can also select any color you like. If you are not satisfied with current picture, just click the eraser button and clear the canvas. You can see other players update in real time and they can see yours too!
## How to run Draw Together!
First, use the following command to enter into app directory:
```
cd paxosapp/app
```
**Note** : Please make sure there is no directory called p3 in the HOME directory. Next, run the script using the following command:
```
./run.sh
```
Wait for the server to start...    
When the server is running, you can visit page: *http://localhost:8080* and *http://localhost:8081* to play the game. Here we assume that there are two servers running on different locations using different paxos nodes. Users will choose the nearest one to visit. Although they connect to different servers, they can still draw a picture together because nodes have to follow Paxos to maintain the consistency.
## Structure of application
Our application has three components: front-end, server, and paxos nodes. Front-end is responsible for handling events (e.g. drawing a line). Servers are responsible for handling users’ requests. Paxos nodes are responsible for storing a specific key-value pair.
The structure of our implementation is as follows:
```
.
|-- app
|   |-- drawtogether.go
|   |-- index.html
|   |-- js
|   |   |-- pixelpals.js
|   |-- run.sh
|-- paxos
    |-- paxos_api.go
     |-- paxos_impl.go
```
pixelpals.js uses AJAX to catch the users’ movement and handle the events. drawtogether.go is our server which responsible for handling users’ requests. And our Paxos logis is implemented in paxos_impl.go.   
The structure of the application is as follows:
![structure](https://github.com/mallocanswer/draw-together/blob/master/Images/3.png)
<img src="https://github.com/mallocanswer/draw-together/blob/master/Images/3.png" width="480">
