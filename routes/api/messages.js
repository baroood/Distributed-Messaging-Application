const express = require('express');
const jwt = require('jsonwebtoken');
const mongoose = require('mongoose');
const axios = require('axios');
const router = express.Router();

const keys = require('../../config/keys');
const verify = require('../../utilities/verify-token');
const Message = require('../../models/Message');
const Conversation = require('../../models/Conversation');
const GlobalMessage = require('../../models/GlobalMessage');

let jwtUser = null;

// Token verfication middleware
router.use(function(req, res, next) {
    try {
        jwtUser = jwt.verify(verify(req), keys.secretOrKey);
        next();
    } catch (err) {
        console.log(err);
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ message: 'Unauthorized' }));
        res.sendStatus(401);
    }
});

// Get global messages
router.get('/global', (req, res) => {
    GlobalMessage.aggregate([
        {
            $lookup: {
                from: 'users',
                localField: 'from',
                foreignField: '_id',
                as: 'fromObj',
            },
        },
    ])
        .project({
            'fromObj.password': 0,
            'fromObj.__v': 0,
            'fromObj.date': 0,
        })
        .exec((err, messages) => {
            if (err) {
                console.log(err);
                res.setHeader('Content-Type', 'application/json');
                res.end(JSON.stringify({ message: 'Failure' }));
                res.sendStatus(500);
            } else {
                res.send(messages);
            }
        });
});

// Post global message
router.post('/global', (req, res) => {
    let message = new GlobalMessage({
        from: jwtUser.id,
        body: req.body.body,
    });

    req.io.sockets.emit('messages', req.body.body);

    message.save(err => {
        if (err) {
            console.log(err);
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ message: 'Failure' }));
            res.sendStatus(500);
        } else {
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ message: 'Success' }));
        }
    });
    axios({
        method: 'post',
        url: 'http://localhost:4000/insert',
        data: {
            senderID: String(message.from),
            conversationID: '99ca2048-a92c-11eb-ba78-b33f5e44d8cd',
            message: String(req.body.body),
        }
    }).then(res => {
        console.log(res.data);
    })

});

// Get conversations list
router.get('/conversations', (req, res) => {
    let from = mongoose.Types.ObjectId(jwtUser.id);
    Conversation.aggregate([
        {
            $lookup: {
                from: 'users',
                localField: 'recipients',
                foreignField: '_id',
                as: 'recipientObj',
            },
        },
    ])
        .match({ recipients: { $all: [{ $elemMatch: { $eq: from } }] } })
        .project({
            'recipientObj.password': 0,
            'recipientObj.__v': 0,
            'recipientObj.date': 0,
        })
        .exec((err, conversations) => {
            if (err) {
                console.log(err);
                res.setHeader('Content-Type', 'application/json');
                res.end(JSON.stringify({ message: 'Failure' }));
                res.sendStatus(500);
            } else {
                res.send(conversations);
            }
        });
});

// Get messages from conversation
// based on to & from
router.get('/conversations/query', (req, res) => {
    let user1 = mongoose.Types.ObjectId(jwtUser.id);
    let user2 = mongoose.Types.ObjectId(req.query.userId);
    Message.aggregate([
        {
            $lookup: {
                from: 'users',
                localField: 'to',
                foreignField: '_id',
                as: 'toObj',
            },
        },
        {
            $lookup: {
                from: 'users',
                localField: 'from',
                foreignField: '_id',
                as: 'fromObj',
            },
        },
    ])
        .match({
            $or: [
                { $and: [{ to: user1 }, { from: user2 }] },
                { $and: [{ to: user2 }, { from: user1 }] },
            ],
        })
        .project({
            'toObj.password': 0,
            'toObj.__v': 0,
            'toObj.date': 0,
            'fromObj.password': 0,
            'fromObj.__v': 0,
            'fromObj.date': 0,
        })
        .exec((err, messages) => {
            if (err) {
                console.log(err);
                res.setHeader('Content-Type', 'application/json');
                res.end(JSON.stringify({ message: 'Failure' }));
                res.sendStatus(500);
            } else {
                res.send(messages);
            }
        });
});

conversationID = "-1"

// Post private message
router.post('/', (req, res) => {
    let from = mongoose.Types.ObjectId(jwtUser.id);
    let to = mongoose.Types.ObjectId(req.body.to);

    Conversation.findOneAndUpdate(
        {
            recipients: {
                $all: [
                    { $elemMatch: { $eq: from } },
                    { $elemMatch: { $eq: to } },
                ],
            },
        },
        {
            recipients: [jwtUser.id, req.body.to],
            lastMessage: req.body.body,
            date: Date.now(),
        },
        { upsert: true, new: true, setDefaultsOnInsert: true },
        function(err, conversation) {
            if (err) {
                console.log(err);
                res.setHeader('Content-Type', 'application/json');
                res.end(JSON.stringify({ message: 'Failure' }));
                res.sendStatus(500);
            } else {
                let message = new Message({
                    conversation: conversation._id,
                    to: req.body.to,
                    from: jwtUser.id,
                    body: req.body.body,
                });

                req.io.sockets.emit('messages', req.body.body);

                message.save(err => {
                    if (err) {
                        console.log(err);
                        res.setHeader('Content-Type', 'application/json');
                        res.end(JSON.stringify({ message: 'Failure' }));
                        res.sendStatus(500);
                    } else {
                        res.setHeader('Content-Type', 'application/json');
                        res.end(
                            JSON.stringify({
                                message: 'Success',
                                conversationId: conversation._id,
                            })
                        );
                    }
                });
            }
        }
    );
    
    console.log(req.body.body);
   
    axios({
        method: 'post',
        url: 'http://localhost:4000/insert',
        data: {
            senderID: String(from),
            conversationID: 'cd4caca2-a8f3-11eb-b90d-b33f5e44d8cd',
            message: String(req.body.body),
        }
    }).then(res => {
        console.log(res.data);
    })
       

});

// delete messages from its id
router.get('/conversations/delete', (req, res) => {
    let msgID = mongoose.Types.ObjectId(req.query.messageId);
    // Message.remove({
    //     "_id" : msgID
    // })
    // .exec((err, messages) => {
    //     if (err) {
    //         console.log(err);
    //         res.setHeader('Content-Type', 'application/json');
    //         res.end(JSON.stringify({ message: 'Failure' }));
    //         res.sendStatus(500);
    //     } else {
    //         console.log("DONE");
    //         res.send(messages);
    //     }
    // });

    GlobalMessage.remove({
        "_id" : msgID
    })
    .exec((err, messages) => {
        if (err) {
            console.log(err);
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ message: 'Failure' }));
            res.sendStatus(500);
        } else {
            console.log("DONE");
            res.send(messages);
        }
    });

    axios({
        method: 'post',
        url: 'http://localhost:4000/deleteMsg',
        data: {
            conversationID: 'cd4caca2-a8f3-11eb-b90d-b33f5e44d8cd',
            chunkNum: 6,
            messageID: "705d6b9e-a925-11eb-95d9-d4258b8c0cb7",
        }
    }).then(res => {
        console.log(res.data);
    })


});

// delete messages from its id
router.post('/conversations/edit', (req, res) => {
    let msgID = mongoose.Types.ObjectId(req.body.messageId);
    let msg = req.body.msg;
    // Message.update(
    //     {"_id" : msgID},
    //     {"body" : msg},
    // )
    // .exec((err, messages) => {
    //     if (err) {
    //         console.log(err);
    //         res.setHeader('Content-Type', 'application/json');
    //         res.end(JSON.stringify({ message: 'Failure' }));
    //         res.sendStatus(500);
    //     } else {
    //         console.log("DONE");
    //         res.send(messages);
    //     }
    // });

    GlobalMessage.update(
        {"_id" : msgID},
        {"body" : msg},
    )
    .exec((err, messages) => {
        if (err) {
            console.log(err);
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ message: 'Failure' }));
            res.sendStatus(500);
        } else {
            console.log("DONE");
            res.send(messages);
        }
    });


    axios({
        method: 'post',
        url: 'http://localhost:4000/updateMsg',
        data: {
            conversationID: 'cd4caca2-a8f3-11eb-b90d-b33f5e44d8cd',
            chunkNum: 6,
            messageID: "705d6b9e-a925-11eb-95d9-d4258b8c0cb7",
            message: msg,
        }
    }).then(res => {
        console.log(res.data);
    })


});

module.exports = router;
