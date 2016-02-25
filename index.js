'use strict';

const config = require('config');
const zmq = require('zmq');
const mongoose = require('mongoose');
const moment = require('moment');

const socket = zmq.socket('sub');

const TickSchema = new mongoose.Schema({}, { strict: false });
const Tick = mongoose.model('ticks', TickSchema, 'ticks');

function init() {
  mongoose.connect(config.get('mongo.uri'), config.get('mongo.options'), err => {
    if(err) return console.error(err);

    socket.connect(config.get('mq.uri'));
    socket.subscribe(config.get('mq.topic'));
    socket.on('message', handleMessage);
  });
}

function handleMessage(topic, data) {
  const message = JSON.parse(data);
  message.time = moment(message.time).toDate();

  const tick = new Tick(message);
  tick.save(err => {
    if(err) return console.error(err);

    console.log('saved - ' + tick.instrument);
  });
}

init();
