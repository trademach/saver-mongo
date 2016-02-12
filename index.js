'use strict';

const config = require('config');
const zmq = require('zmq');
const mongoose = require('mongoose');

const socket = zmq.socket('sub');

const TickSchema = new mongoose.Schema({}, { strict: false });
const Tick = mongoose.model('ticks', TickSchema, 'ticks');

function init() {
  mongoose.connect(config.get('mongo.uri'), config.get('mongo.options'), err => {
    if(err) return console.error(err.stack);

    // subscribe to all
    socket.connect(config.get('mq.uri'));
    socket.subscribe('');
    socket.on('message', handleMessage);
  });
}

function handleMessage(topic, data) {
  const message = JSON.parse(data);
  message.time = new Date(message.time);

  const tick = new Tick(message);
  tick.save(err => {
    if(err) return console.error(err);

    console.log('saved - ' + tick.instrument);
  });
}

init();
