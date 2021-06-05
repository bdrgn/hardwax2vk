# Import libraries
import os
import requests
import boto3
from boto3.dynamodb.conditions import Key
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import datetime
import vk_api
from vk_api.audio import VkAudio
import time
import textdistance
from collections import OrderedDict

# Get environmental variables from AWS Lambda
vk_login = os.environ['vk_login']
vk_password = os.environ['vk_password']
user_id = os.environ['user_id']
group_id = os.environ['group_id']
app_id = os.environ['app_id']

# Connect to DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table('HardwaxLinks')

# Connect to VK.com
vk_session = vk_api.VkApi(vk_login, vk_password, app_id=app_id)
vk_session.auth()
vk = vk_session.get_api()
vkaudio = VkAudio(vk_session)
tools = vk_api.VkTools(vk_session)

def get_link_status(record_link):

    """
    Get the status of link: whether if was posted or tried.
    (to not post the link twice and not even attempt to post when tracks are missing in VK)
    :param record_link: the link (str) to release at hardwax.com.
    :return: the status (str) for the link. (whether if was posted or tried)
    """

    # Get response for the link from DynamoDB
    response = table.query(KeyConditionExpression=Key('Link').eq(record_link))

    # If the number of returned items is 0 then the link was not attempted before else get the status
    if len(response['Items']) == 0:
        Result = 'Not Tried'
    else:
        Result = response['Items'][-1]['Result']

    return Result

def get_records_page(page_url):
    """
    Parse a single page of hardwax.com catalogue.
    :param page_url: the link (str) to catalogue page at hardwax.com.
    :return: the dataframe with links to releases as well as artists, titles, labels and so on.
    """

    # Print the status so it will be available in logs
    print(f'Getting {page_url}')

    # Get soup of webpage
    response = requests.get(page_url).content
    soup = BeautifulSoup(response, "html.parser")

    # Filter soup
    records_info = soup.findAll("div", {"class": "linebig"})

    # Define function to get artist and title from record info
    def f(x):
        try:
            children = list(x.children)

            record_title = children[1]

            record_artist = list(children[0].children)[0].string

            return record_artist, record_title
        except:
            return ()

    # Process all the records data
    record_names = [x for x in map(f, records_info) if len(x) > 0]
    
    record_data = []
    for x in [(x.get('href'), x.get('href')[1:6]) for x in soup.findAll('a') if x.get('href')[1:6].isdigit()]:
        if x not in record_data:
            record_data.append(x)

    # Create a records df
    records_df = pd.DataFrame([x[0] + x[1] for x in zip(record_data, record_names)], \
                              columns=['record_link', 'record_id', 'record_artist', 'record_title'])

    # Order the columns in the df
    records_df = records_df[['record_id', 'record_artist', 'record_title', 'record_link']]

    # Create title column using artist and title
    records_df['record_title'] = records_df['record_artist'] + ' ' + records_df['record_title']

    # Clean the artist column
    records_df['record_artist'] = records_df['record_artist'].str[:-1]

    # Add label data
    label_data = [(x.get('href'), x.string) for x in soup.findAll('a') \
                  if '/label/' in x.get('href') and x.string is not None]
    records_df['label_link'] = [x[0] for x in label_data]
    records_df['label'] = [x[1] for x in label_data]

    # Add page number
    page_number = int(page_url[page_url.index('page=') + 5:])
    records_df['page_number'] = page_number

    records_df['record_link'] = 'https://hardwax.com' + records_df['record_link']

    # Return the df
    return records_df


def get_record_tracks_hardwax(record_link):
    """
    Get the *.mp3 track titles from link to the release page.
    :param record_link: the link (str) to release page at hardwax.com.
    :return: a set of titles of *.mp3 tracks.
    """

    # Get soup of webpage
    response = requests.get(record_link).content
    soup = BeautifulSoup(response, "html.parser")

    # Get all the links with .mp3 in them
    track_names = []
    track_links_dict = OrderedDict()

    for link in soup.findAll('a'):
        link_str = link.get('href')
        link_title_str = link.get('title')

        if '.mp3' in link_str and link_title_str.upper() not in track_names and 'clip' not in link_str:
            track_names.append(link_title_str.upper())

            track_links_dict[link_title_str] = link_str

    return track_links_dict


def get_record_images_hardwax(record_link):
    """
    Get links to cover images from hardwax.com release URL.
    :param record_link: the link (str) to release page at hardwax.com.
    :return: a set of URLs to cover images.
    """

    # Get soup of webpage
    response = requests.get(record_link).content
    soup = BeautifulSoup(response, "html.parser")


    # Iterate over image URL and find release covers
    cover_image_links = []

    for img in soup.findAll('img'):
        cover_image_links.append(img.get('src'))

    cover_image_links = [x for x in cover_image_links if 'big' in x]

    return cover_image_links

def get_audio_id(record_track):
    """
    Get track owner id and track id for an *.mp3 in VK.
    :param record_track: *.mp3 track title (str).
    :return: tuple of track id from VK and track owner id from VK.s
    """

    print(f'Searching for {record_track} in VK')

    # Stop for domain-specific edge cases
    if record_track.split(': ')[1].lower() in ['version'] or record_track.split(': ')[0].lower() in ['unknown']:
        print(f"{record_track} -> None")
        return None

    # Search VK for needed tracks,
    try:
        # Search VK
        search_result = vkaudio.search(record_track)

        # Wait 7 seconds to avoid cooldown
        time.sleep(7)

        # Search until the Levenstein distance between desired track title and VK title is 0
        min_lev_dist = 10000
        while True:

            # Handle possible VK search exceptions
            try:
                track = next(search_result)
            except StopIteration:
                break
            except AttributeError:
                raise ValueError

            # Preprocess track title of VK and query to make them comparable with Levenstein distance
            track_title = track['artist'] + ': ' + track['title']
            lev_dist = textdistance.levenshtein(record_track.upper().replace(' ', ''),
                                        track_title.upper().replace(' ', ''))
            min_lev_dist = min(lev_dist, min_lev_dist)
            if min_lev_dist == 0:
                print(f"{record_track} -> {track['artist']}: {track['title']}")
                return track['owner_id'], track['id']

        # Print summary if the track cannot be found at the moment
        print(f"{record_track} -> None; Min Lev. dist.: {min_lev_dist}.")

    # Handle exception with cooldown if it occurs
    except AttributeError as err:
        print(f"{record_track} -> None")
        print(f'Track posting exception {record_track} (Attribute error)')
        raise

def upload_photos(record_covers_links):
    """
    Upload photos to VK.
    :param record_covers_links: URLs of images from hardwax.com.
    :return: VK images ids of uploaded images.
    """
    # Download images from links to files
    upload_filenames = []
    counter = 0
    for pic_link in record_covers_links:
        filename = f'photo_{counter}.jpg'
        counter += 1
        f = open(filename, 'wb')
        try:
            f.write(requests.get(pic_link).content)
        except:
            continue
            print('Photos upload exception')
        upload_filenames.append(filename)
        f.close()

    # Upload images to VK and save IDs
    upload = vk_api.VkUpload(vk_session)
    uploaded_photos_data = []
    for filename in upload_filenames:
        photo = upload.photo_wall(filename, group_id=group_id, user_id=user_id)

        photo_owner_id, photo_id = photo[0]['owner_id'], photo[0]['id']

        uploaded_photos_data.append((photo_owner_id, photo_id))

    # Delete redundant image files
    for filename in upload_filenames:
        os.remove(filename)

    # Return image links
    return uploaded_photos_data

def get_title_label_link_hardwax(record_link):
    """
    Get title, label and link for hardwax.com release.
    :param record_link: the link (str) to release page at hardwax.com.
    :return: title and label of release at hardwax.com.
    """

    # Get soup of webpage
    response = requests.get(record_link).content
    soup = BeautifulSoup(response, "html.parser")

    # Find record label for a release
    for link in soup.findAll('a'):
        link_str = link.get('href')
        if 'label' in link_str:
            record_label = [x for x in link.children][0]

        if link.get('title') == None and str.isdigit(link_str[1:6]):
            record_link = 'https://hardwax.com' + link_str

    # Get artist and title of release
    page_name = soup.title.string.replace(' - Hard Wax', '')
    split_index = page_name.index(':')
    artist, title = page_name[:split_index], page_name[split_index + 1:].strip(' ')

    return f'{artist}: {title}', record_label

def post_record(record_link):
    """
    Post hardwax.com release to VK community with *.mp3 tracks, cover image and text metadata for title, label and link.
    :param record_link: the link (str) to release page at hardwax.com.
    :return: whether the release was posted to VK or not. (str)
    """

    # Check if the link was previously posted or attempted
    link_status = get_link_status(record_link)
    if link_status == 'Posted' or link_status == 'Tried':
        return 'Not posted'

    # Get info by link
    record_name, record_label = get_title_label_link_hardwax(record_link=record_link)

    # Print to logs if script attempts to post a record
    print(f'Posting record: {record_name}')

    # Get record tracks (not post if there are no track, e. g. merchandise, t-shirts, etc.)
    record_tracks = [f"{x.split(': ')[0]}: {x.split(': ')[1]}" for x in get_record_tracks_hardwax(record_link)]

    if len(record_tracks) == 0:
        return 'Not posted'

    # Get image links
    record_images = get_record_images_hardwax(record_link)

    # Search for release tracks in VK
    try:
        track_ids = []
        not_included_tracks = []

        for i, track in enumerate(record_tracks):

            try:
                result = get_audio_id(track)
            except ValueError:
                return 'VK Glitch'

            if result is not None and result not in track_ids:
                track_ids.append(result)
            else:
                not_included_tracks.append(track)

            if len(track_ids) >= 9:
                break

            max_possible_tracks = (len(record_tracks) - i - 1) + len(track_ids)

            # If the if not enough tracks in VK update status in DynamoDB and stop posting
            if max_possible_tracks < len(record_tracks) and max_possible_tracks < 9:

                # Write message to logs
                print(f'Not enough found tracks: {record_name}')

                # Update link status in DynamoDB
                table.put_item(Item={'Result': 'Tried', 'Link': record_link})

                return 'Not Posted'
    except Exception as e:
        print(f"Can't get tracks from VK: {str(e.args)}")
        return 'Not posted'

    # Upload photos to VK
    try:
        photo_ids = upload_photos([record_images[0]])
    except Exception as e:
        print(f"Can't upload photos: {str(e.args)}")
        return 'Not posted'

    # Generate record message
    record_message = \
        f"""
Title: {record_name}
Label: {record_label}
Release link: {record_link}
        """

    # Set attachment string
    track_owners_and_ids = [f'audio{track_owner}_{track_id}' for track_owner, track_id in track_ids]
    track_owners_and_ids = track_owners_and_ids[:9]
    track_string = ','.join(track_owners_and_ids)
    photo_ids = photo_ids[:10 - len(track_owners_and_ids)]
    photos_string = ','.join([f'photo{photo_owner_id}_{photo_id}' for photo_owner_id, photo_id in photo_ids])
    attachment_string = track_string + ',' + photos_string

    # Post to VK
    vk.wall.post(owner_id='-183970488', from_community=1, message=record_message, attachments=attachment_string)

    # Write status to DynamoDB
    table.put_item(Item={'Result': 'Posted', 'Link': record_link})

    print(f'Posted: {record_name}')
    return 'Posted'

def update_pinned_post():
    """
    Pin the most popular post from last week.
    :return: None.
    """

    latest_wall = tools.get_all_slow_iter(method='wall.get', max_count=35, values={'owner_id': -183970488})
    latests_posts = []
    for i in range(1):
        latests_posts.append(next(latest_wall))
    pinned_posts = [x for x in latests_posts if x.get('is_pinned') == 1]
    for pinned_post in pinned_posts:
        vk.wall.unpin(owner_id='-183970488', post_id=str(pinned_post['id']))

    latest_wall = tools.get_all_slow_iter(method='wall.get', max_count=35, values={'owner_id': -183970488})
    top_post = 0
    top_likes = 0
    for i in range(35):
        x = next(latest_wall)
        if top_likes == 0:
            top_post = x['id']
            top_likes = x['likes']['count']
            continue
        if x['likes']['count'] > top_likes:
            top_post = x['id']
            top_likes = x['likes']['count']
    vk.wall.pin(owner_id='-183970488', post_id=str(top_post))

def lambda_handler(event, context):
    """
    Post another hardwax.com release to VK community.
    :param event: random values (not used).
    :param context: random values (not used).
    :return: whether something was posted or not.
    """
    
    # Work in the AWS Lambda's tmp directory
    os.chdir('/tmp')
    
    # if 'https' in event['message']['text']:
    #     try:
    #         reply = event['message']['text']
    #         send_message(reply, chat_id)
    
    #         posting_result = post_record(reply)
    
    #         send_message(posting_result, chat_id)
    
    #         return posting_result
    #     except:
    #         return None

    # Update the pinned post in VK community
    update_pinned_post()

    # Add section URLS to search for releases in
    main_pages = \
            [
                'https://hardwax.com/?page={}', 
                'https://hardwax.com/this-week/?page={}', 
                'https://hardwax.com/last-week/?page={}', 
                'https://hardwax.com/downloads/?page={}'
            ] # hardwax.com main pages

    # Search in yearly charts with 50% probability (to avoid digging deep in the past)
    if np.random.random() >= 2:
        main_pages = main_pages +\
        ['https://hardwax.com/charts-' + str(x) + '/?page={}'
                         for x in range(datetime.datetime.now().year - 1, 2006, -1)]

    # Continue with most popular and important sections
    important_sections = [
        'https://hardwax.com/techno/?page={}',
        'https://hardwax.com/basic-channel/?page={}',
        'https://hardwax.com/chicago-oldschool/?page={}',
        'https://hardwax.com/digital/?page={}',
        'https://hardwax.com/detroit-house/?page={}',
        'https://hardwax.com/drum-n-bass/?page={}',
        'https://hardwax.com/grime/?page={}',
        'https://hardwax.com/house/?page={}',
        'https://hardwax.com/disco/?page={}',
        'https://hardwax.com/essentials/?page={}',
        'https://hardwax.com/exclusives/?page={}',
        'https://hardwax.com/honest-jons/?page={}',
        'https://hardwax.com/new-wave/?page={}',
        'https://hardwax.com/outernational/?page={}',
        'https://hardwax.com/section/reggae/?page={}']
    np.random.shuffle(important_sections)

    # If everything from previous sections was posted revert to less popular (and more weird)
    other_sections = [
        'https://hardwax.com/surgeon/?page={}',
        'https://hardwax.com/collectors-items/?page={}',
        'https://hardwax.com/colundi-everyone/?page={}',
        'https://hardwax.com/drexciya/?page={}',
        'https://hardwax.com/early-electronic/?page={}',
        'https://hardwax.com/electro/?page={}',
        'https://hardwax.com/electronic/?page={}',
        'https://hardwax.com/irdial-discs/?page={}',
        'https://hardwax.com/mego/?page={}',
        'https://hardwax.com/reissues/?page={}',
        'https://hardwax.com/section/d/?page={}',
        'https://hardwax.com/section/euro/?page={}',
        'https://hardwax.com/section/uk/?page={}',
        'https://hardwax.com/section/us/?page={}',
        'https://hardwax.com/this-week/?page={}',
        'https://hardwax.com/last-week/?page={}',
        'https://hardwax.com/back-in-stock/?page={}',
        'https://hardwax.com/downloads/?page={}'
    ]
    np.random.shuffle(other_sections)

    # Get the final queue of sections
    sections = important_sections + other_sections
    
    # Define a generator for sections as well as excluded sections list
    excluded_sections = []
    def sections_generator(sections):
        for page in range(1, 1001):
            for section in main_pages:
                if section.split('?')[0] not in excluded_sections:
                    yield section.format(page)
                    
        for page in range(1, 1001):
            for section in sections:
                if section.split('?')[0] not in excluded_sections:
                    yield section.format(page)
    sections_generator_object = sections_generator(sections)

    # Iterate over sections queue and attempt to post every release which was not previously posted or attempted
    for section in sections_generator_object:

        # Get a data frame of releases from section URL
        df = get_records_page(section)

        # Stop looking in a section if there are no more releases
        if df.shape[0] == 0:
            excluded_sections.append(section.split('?')[0])

        # Iterate over record links and attempt posting
        for record_link in df['record_link']:

            result = post_record(record_link)

            # If succeed - stop
            if result == 'Posted':
                return 'Posted'

            # If VK glitches (rarely), stop bothering the API
            if result == 'VK Glitch':
                return 'Stop posting'
