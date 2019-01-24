package com.neohope.kks.demo.clickstreamenrich;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neohope.kks.demo.clickstreamenrich.model.PageView;
import com.neohope.kks.demo.clickstreamenrich.model.Search;
import com.neohope.kks.demo.clickstreamenrich.model.UserActivity;
import com.neohope.kks.demo.clickstreamenrich.model.UserProfile;
import com.neohope.kks.demo.serde.JsonDeserializer;
import com.neohope.kks.demo.serde.JsonSerializer;
import com.neohope.kks.demo.serde.WrapperSerde;

import java.time.Duration;
import java.util.Properties;

/**
 * 读入三个主题（USER_PROFILE_TOPIC，PAGE_VIEW_TOPIC，SEARCH_TOPIC）
 * 计算结果，并将结果输出到USER_ACTIVITY_TOPIC
 * @author Hansen
 */
public class StreamClickstreamEnrichment {
	
    public static final String USER_PROFILE_TOPIC = "clicks.user.profile";
    public static final String PAGE_VIEW_TOPIC = "clicks.pages.views";
    public static final String SEARCH_TOPIC = "clicks.search";
    public static final String USER_ACTIVITY_TOPIC = "clicks.user.activity";
    
    private static Logger logger = LoggerFactory.getLogger(StreamClickstreamEnrichment.class);

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "clicks");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        Consumed<Integer, PageView> consumedPage = Consumed.with(Serdes.Integer(), new PageViewSerde()).withTimestampExtractor(new LogAndSkipOnInvalidTimestamp());
        Consumed<Integer, UserProfile> consumedProfile = Consumed.with(Serdes.Integer(), new ProfileSerde()).withTimestampExtractor(new LogAndSkipOnInvalidTimestamp());
        Consumed<Integer, Search> consumedSearch = Consumed.with(Serdes.Integer(), new SearchSerde()).withTimestampExtractor(new LogAndSkipOnInvalidTimestamp());
        KStream<Integer, PageView> views = builder.stream(PAGE_VIEW_TOPIC, consumedPage);
        KTable<Integer, UserProfile> profiles = builder.table(USER_PROFILE_TOPIC, consumedProfile);
        KStream<Integer, Search> searches = builder.stream(SEARCH_TOPIC, consumedSearch);

        KStream<Integer, UserActivity> viewsWithProfile = views.leftJoin(profiles,
                    (page, profile) -> {
                        if (profile != null)
                            return new UserActivity(profile.getUserID(), profile.getUserName(), profile.getZipcode(), profile.getInterests(), "", page.getPage());
                        else
                           return new UserActivity(-1, "", "", null, "", page.getPage());

        });

        Joined<Integer,UserActivity,Search> joinedActitityAndSearch = Joined.with(Serdes.Integer(), new UserActivitySerde(), new SearchSerde());
        KStream<Integer, UserActivity> userActivityKStream = viewsWithProfile.leftJoin(searches,
                (userActivity, search) -> {
                    if (search != null) userActivity.updateSearch(search.getSearchTerms());
                    else userActivity.updateSearch("");
                    return userActivity;
                },
                JoinWindows.of(Duration.ofMillis(1000)), joinedActitityAndSearch);

        Produced<Integer, UserActivity> consumedUserActtity = Produced.with(Serdes.Integer(), new UserActivitySerde());
        userActivityKStream.to(USER_ACTIVITY_TOPIC, consumedUserActtity);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    	logger.info(">>>>>>ClickstreamEnrichment started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>ClickstreamEnrichment exited");

        streams.close();
    }

    static public final class PageViewSerde extends WrapperSerde<PageView> {
        public PageViewSerde() {
            super(new JsonSerializer<PageView>(), new JsonDeserializer<PageView>(PageView.class));
        }
    }

    static public final class ProfileSerde extends WrapperSerde<UserProfile> {
        public ProfileSerde() {
            super(new JsonSerializer<UserProfile>(), new JsonDeserializer<UserProfile>(UserProfile.class));
        }
    }

    static public final class SearchSerde extends WrapperSerde<Search> {
        public SearchSerde() {
            super(new JsonSerializer<Search>(), new JsonDeserializer<Search>(Search.class));
        }
    }

    static public final class UserActivitySerde extends WrapperSerde<UserActivity> {
        public UserActivitySerde() {
            super(new JsonSerializer<UserActivity>(), new JsonDeserializer<UserActivity>(UserActivity.class));
        }
    }
}
