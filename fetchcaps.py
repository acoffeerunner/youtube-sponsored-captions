import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, VideoUnavailable, TranscriptsDisabled
from collections import OrderedDict

def fetch_timed_captions(video_id, startTime, endTime):

  startTime = list(startTime)
  endTime = list(endTime)
  try:
      # fetch english transcription (manual(preferred)/auto-gen) for video_id
      transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
      transcript = transcript_list.find_transcript(['en'])
      trans = transcript.fetch()

      final_list = []
      
      # current element in list
      i = 0
      
      for idx, ele in enumerate(trans):

        # rework returned dict a little to have startTime and endTime keys
        ele['startTime'] = ele['start']
        ele['endTime'] = round(ele['startTime']+ele['duration'], 2)
        del ele['start']
        del ele['duration']
        
        # if caption in sponsored time stamp, label it as 1. else, label it as 0.
        if startTime[i] <= ele['startTime'] <= endTime[i]:
            ele['isSponsored'] = 1
            i = i + 1 if i < len(startTime)-1 else i
        else:
            ele['isSponsored'] = 0

        # add videoID to dict
        ele['videoID'] = video_id

        # reorder dict keys
        key_order = ['videoID', 'startTime', 'endTime', 'text', 'isSponsored']
        ele = OrderedDict(ele)
        for k in key_order:
            ele.move_to_end(k)
        ele = dict(ele)

        # append reordered dict to final_list
        final_list.append(ele)

      return final_list
  
  # exceptions thrown by youtube_transcript_api
  except NoTranscriptFound:
      print('[ERROR] error while getting transcriptions for '+video_id+'. Reason: No en captions')
  
  except VideoUnavailable:
      print('[ERROR] error while getting transcriptions for ' + video_id+'. Reason: Video Unavailable')
  
  except TranscriptsDisabled:
      print('[ERROR] error while getting transcriptions for ' + video_id + '. Reason: Disabled Transcripts')
  
  except Exception as e:
      print('[ERROR] error while getting transcriptions for ' + video_id + '. Reason: '+str(e.__str__()))

def process_video(video_id, df_final, df_lock, counter_lock, processed_count, remaining_count):
    sponsorTimes_video = sponsorTimes.loc[sponsorTimes['videoID'] == video_id]
    startTime = sponsorTimes_video['startTime'].sort_values()
    endTime = sponsorTimes_video['endTime'].sort_values()

    timed_captions = fetch_timed_captions(video_id, startTime, endTime)
    if timed_captions:
        new_df = pd.DataFrame(timed_captions)
        # Use lock to ensure thread-safe DataFrame update
        with df_lock:
            df_final = pd.concat([df_final, new_df], ignore_index=True)
            df_final.to_csv("captions1.csv", mode='a', index=False, header=False)

        # Update the processed and remaining counts
        with counter_lock:
            processed_count.value += 1
            remaining_count.value = len(videoList) - processed_count.value

        print('[INFO] retrieved captions for ' + video_id + ', elapsed: ' + str(processed_count.value) +
              ', left: ' + str(remaining_count.value))

    return df_final

start = time.time()

# read video URIs and sponsor spot time stamps from the CSVs
videosList = pd.read_csv("videoList.csv")
sponsorTimes = pd.read_csv("sponsorTimes_smushhsums.csv")
videoList = videosList['videoID'].tolist()

# init empty df for sponsored and non-sponsored captions
df_final = pd.DataFrame()
n = 0

# thread-safety stuff
df_lock = threading.Lock()
counter_lock = threading.Lock()

# counters
processed_count = multiprocessing.Value('i', 0)
remaining_count = multiprocessing.Value('i', len(videoList))

with ThreadPoolExecutor() as executor:
    threads = [
        executor.submit(process_video, vid, df_final.copy(), df_lock, counter_lock, processed_count, remaining_count)
        for vid in videoList]

    for thread in threads:
        df_final = thread.result()

print("\n\n\n-------------------------------------------------")
print('runtime in seconds='+str(time.time()-start))
print("-------------------------------------------------")