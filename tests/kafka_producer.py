from time import sleep
from json import dumps
from kafka import KafkaProducer
from faker import Faker
import argparse
import random

parser = argparse.ArgumentParser()
parser.add_argument("--records_per_min", help="number of records to publish per minute")
args = parser.parse_args()


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

topic = 'tweets'
fake = Faker()
records_per_mins = int(args.records_per_min)     # records per minute
hashtags = ['#panasonicindia', '#sonyindia',
            '#LGLiving', '#xiaomi', '#huawei', '#LGLifeIsGood', '#SonySupport', '#panasonicsupport']
sample_places = [   ('IN', (28.704060, 77.102493)), 
                    ('AU', (-25.274399, 133.775131)),
                    ('US', (37.090240, -95.712891))
                ]

sample_text = [
    'I purchased a TV few months back but it has a fault now.',
    'customer support engineer tried to fix the faulty TV',
    'I am not sure what is wrong with this TV. Need immediate help',
    'Disposed my laptop successfully with the help of @PanasonicIndia #DiwaliwaliSafai #PanasonicIndia Join @chidambar08 @dayalojha_ @AswaniJaishree',
    'Amazing experience of watching ipl on smart tv.'
]

while True:
    next_tick_time = 60 / records_per_mins
    # name = fake.name()
    hashtag = random.choice(hashtags)
    # place = random.choice(sample_places)
    text = random.choice(sample_text)
    
    data = {
        "created_at":"Fri Feb 04 06:12:11 +0000 2022",
        "id":1489481892885934083,
        "id_str":"1489481892885934083",
        "text": text,
        "source":"\u003ca href=\"http:\/\/twitter.com\/download\/android\" rel=\"nofollow\"\u003eTwitter for Android\u003c\/a\u003e",
        "truncated":False,"in_reply_to_status_id":None,"in_reply_to_status_id_str":None,"in_reply_to_user_id":None,"in_reply_to_user_id_str":None,"in_reply_to_screen_name":None,"user":{"id":1934503716,"id_str":"1934503716","name":"Rudi A.R.","screen_name":"rudipitt","location":None,"url":None,"description":"Games-Games-Games...","translator_type":"none","protected":False,"verified":False,"followers_count":3245,"friends_count":2663,"listed_count":35,"favourites_count":6007,"statuses_count":4129,"created_at":"Fri Oct 04 15:08:36 +0000 2013","utc_offset":None,"time_zone":None,"geo_enabled":False,"lang":None,"contributors_enabled":False,"is_translator":False,"profile_background_color":"C0DEED","profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_tile":False,"profile_link_color":"1DA1F2","profile_sidebar_border_color":"C0DEED","profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":True,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/420954950798409728\/94V0ojuH_normal.jpeg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/420954950798409728\/94V0ojuH_normal.jpeg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/1934503716\/1389198454","default_profile":"","default_profile_image":False,"following":None,"follow_request_sent":None,"notifications":None,"withheld_in_countries":[]},"geo":None,"coordinates":None,"place":None,"contributors":None,"retweeted_status":{"created_at":"Fri Sep 10 10:49:54 +0000 2021","id":1436280766590242826,"id_str":"1436280766590242826","text":"God Of War Ragnarok - PlayStation Showcase 2021 Reveal Trailer | PS5\nhttps:\/\/t.co\/YT9KCnjt0G #GodOfWarRagnarok\u2026 https:\/\/t.co\/O5R367xYyB","source":"\u003ca href=\"http:\/\/twitter.com\/download\/android\" rel=\"nofollow\"\u003eTwitter for Android\u003c\/a\u003e","truncated":True,"in_reply_to_status_id":None,"in_reply_to_status_id_str":None,"in_reply_to_user_id":None,"in_reply_to_user_id_str":None,"in_reply_to_screen_name":None,"user":{"id":4354197195,"id_str":"4354197195","name":"DeadArtic Games\u2122","screen_name":"DeadarticGames","location":None,"url":"http:\/\/www.deadarticgames.com","description":"\ud83c\udfae Videogames website \ud83c\udfae                 http:\/\/instagram.com\/deadarticgames\/ \nhttp:\/\/facebook.com\/deadarticgames\/              \ud83d\udce7 contacto@deadarticgames.com","translator_type":"none","protected":False,"verified":False,"followers_count":4279,"friends_count":2941,"listed_count":34,"favourites_count":337,"statuses_count":5724,"created_at":"Wed Dec 02 20:52:55 +0000 2015","utc_offset":None,"time_zone":None,"geo_enabled":False,"lang":None,"contributors_enabled":False,"is_translator":False,"profile_background_color":"C0DEED","profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_tile":False,"profile_link_color":"1DA1F2","profile_sidebar_border_color":"C0DEED","profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":True,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/684104058873982976\/s1FZwCqH_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/684104058873982976\/s1FZwCqH_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/4354197195\/1471951752","default_profile":True,"default_profile_image":False,"following":None,"follow_request_sent":None,"notifications":None,"withheld_in_countries":[]},"geo":None,"coordinates":None,"place":None,"contributors":None,"is_quote_status":False,"extended_tweet":{"full_text":"God Of War Ragnarok - PlayStation Showcase 2021 Reveal Trailer | PS5\nhttps:\/\/t.co\/YT9KCnjt0G #GodOfWarRagnarok #GodofWar #PlayStationShowcase2021 #PlayStationShowcase #PlayStation5 #PS5 #Ps4Pro #Ps4 #PlayStation #Sony #deadarticgames","display_text_range":[0,233],
        "entities":{
            "hashtags":[
                { "text":hashtag,"indices":[93,110] },
            ],
            "urls":[{"url":"https:\/\/t.co\/YT9KCnjt0G","expanded_url":"https:\/\/www.deadarticgames.com\/2021\/09\/god-of-war-ragnarok-playstation.html","display_url":"deadarticgames.com\/2021\/09\/god-of\u2026","indices":[69,92]}],
            "user_mentions":[],
            "symbols":[]
            }
        },
        "quote_count":0,
        "reply_count":0,
        "retweet_count":2,
        "favorite_count":1,
            "entities":{
                "hashtags":[
                        {"text": hashtag,"indices":[93,110]}
                    ],
                    "urls":[
                        {"url":"https:\/\/t.co\/YT9KCnjt0G","expanded_url":"https:\/\/www.deadarticgames.com\/2021\/09\/god-of-war-ragnarok-playstation.html","display_url":"deadarticgames.com\/2021\/09\/god-of\u2026","indices":[69,92]},
                        {"url":"https:\/\/t.co\/O5R367xYyB","expanded_url":"https:\/\/twitter.com\/i\/web\/status\/1436280766590242826","display_url":"twitter.com\/i\/web\/status\/1\u2026","indices":[112,135]}],
                    "user_mentions":[],
                    "symbols":[]},
                    "favorited":False,
                    "retweeted":False,
                    "possibly_sensitive":False,
                    "filter_level":"low",
                    "lang":"en"},
                    "is_quote_status":False,
                    "quote_count":0,
                    "reply_count":0,
                    "retweet_count":0,
                    "favorite_count":0,
                    "entities":{
                        "hashtags":[
                            {"text":hashtag,"indices":[113,130]}],
                            "urls":[{"url":"https:\/\/t.co\/YT9KCnjt0G","expanded_url":"https:\/\/www.deadarticgames.com\/2021\/09\/god-of-war-ragnarok-playstation.html","display_url":"deadarticgames.com\/2021\/09\/god-of\u2026","indices":[89,112]}],"user_mentions":[{"screen_name":"DeadarticGames","name":"DeadArtic Games\u2122","id":4354197195,"id_str":"4354197195","indices":[3,18]}],"symbols":[]},"favorited":False,"retweeted":False,"possibly_sensitive":False,"filter_level":"low","lang":"en","timestamp_ms":"1643955131156"}
    print(f'Text:{text}\tHashtag: {hashtag}')
    producer.send(topic, data)
    sleep(next_tick_time)




