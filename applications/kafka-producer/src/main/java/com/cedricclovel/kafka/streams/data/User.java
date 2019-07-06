package com.cedricclovel.kafka.streams.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class User {

    @JsonProperty("id")
    public Long id;

    @JsonProperty("name")
    public String name;

    @JsonProperty("screen_name")
    public String screenName;

    @JsonProperty("followers_count")
    public Integer followersCount;

    @JsonProperty("friends_count")
    public Integer friendsCount;

    @JsonProperty("statuses_count")
    public Integer statusesCount;
}
