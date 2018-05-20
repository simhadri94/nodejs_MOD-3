//const express = require('express');
const mongodb= require('mongodb')
const fs = require('fs');
const async = require('async')

const url = 'mongodb://localhost:27017/bitcoin_Users-db'

var customer_data =  JSON.parse(fs.readFileSync('m3-customer-data.json', 'utf8'));

var cust_address =  JSON.parse(fs.readFileSync('m3-customer-address-data.json', 'utf8'));

let tasks = []
const limit = parseInt(process.argv[2]) || 1000

mongodb.MongoClient.connect(url, (error, client) => {
  if (error) return process.exit(1)

  var db = client.db('bitcoin_Users-db')

  customer_data.forEach((customer,index,list) => {
  	customer_data[index] = Object.assign(customer,cust_address[index])

  	if(index % limit == 0 ) {
  		var start = index
  		var end = (start + limit > customer_data.length) ? customer_data.length - 1 : start + limit

  		tasks.push((done) => {
  			db.collection('customers').insert(customer_data.slice(start,end),(err,results)=>{
  				done(err,results)
  			})
  		})
  	}
  })
  console.log(`Launching ${tasks.length} parallel task(s)`)
  const startTime = Date.now()

  async.parallel(tasks, (error, results) => {
    if (error) console.error(error)
    const endTime = Date.now()
    console.log(`Execution time: ${endTime-startTime}`)
    // console.log(results)
    client.close()
  })

  })



