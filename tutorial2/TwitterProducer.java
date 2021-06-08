package com.ghaed.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {
    public TwitterProducer(){}

    public static void main(String[] args) {
        System.out.println("hello world");
        new TwitterProducer().run();

    }

    public void run(){
        //create Twitter client

        //Create a Kafka producer

        //Loop to send tweets to Kafka
    }

    String consumerKey = "nm9MzPumtJhz1NOtUwctiLNWZ";
    String consumerSecret = "U2WDZ9Edj8rlZUdhYPH8QUwaiKtZuHnUXP9ETnjRSLsKDjG6j4";
    String token = "3424846481-9tcsnxMo9jqUlgjKt3llVknl0qJo1Tx1DaoCgSc";
    String secret = "y4yzENr9hp5Cxmt62QzMGEYIhI7QDYDHvwz93EefLMLYO";

    public void createTwitterClient(){
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
    }
}
