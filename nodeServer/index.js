const http = require('http');
const socketIO = require('socket.io');

const server = http.createServer((req, res) => {
    // Set up the Access-Control-Allow-Origin header
    res.setHeader('Access-Control-Allow-Origin', 'http://127.0.0.1:5500'); // Replace with your frontend's URL
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    // Handle other parts of your server's logic
    res.end('Hello from the server!');
});

const io = socketIO(server, {
    cors : {
        origin : "*"
    }
});

const users = {};

io.on('connection', socket => {
    console.log("Mani");

    socket.on('new-user-joined', name => {
        console.log(name);
        users[socket.id] = name;
        socket.broadcast.emit('user-joined', name);
    });

    socket.on('send', message => {
        socket.broadcast.emit('receive', { message: message, name: users[socket.id] });
    });

    socket.on('disconnect', message =>{
        socket.broadcast.emit('left', users[socket.id]);
        delete users[socket.id];
    })
});

const PORT = 8000;
server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
