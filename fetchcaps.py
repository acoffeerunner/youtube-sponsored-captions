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


# read video URIs and sponsor spot time stamps from the CSVs
videosList = pd.read_csv("videoList.csv")
sponsorTimes = pd.read_csv("sponsorTimes_smushhsums.csv")
videoList = videosList['videoID'].tolist()

# init empty df for sponsored and non-sponsored captions
df_final = pd.DataFrame()

videoCount = 0

for i in videoList:
    video_id = i

    # find all sponsor spot time stamps for this video URI
    sponsorTimes_video = sponsorTimes.loc[sponsorTimes['videoID'] == video_id]

    # sort by times in ascending order 
    startTime = sponsorTimes_video['startTime'].sort_values()
    endTime = sponsorTimes_video['endTime'].sort_values()


    timed_captions = fetch_timed_captions(video_id, startTime, endTime)
    
    videoCount = videoCount + 1

    if timed_captions:
        df_int = pd.DataFrame(timed_captions)
        print('[INFO] retrieved captions for ' + video_id +', elapsed: ' + str(videoCount) +', left: ' + str(len(videoList) - videoCount))

        # append this video's data to the final DataFrame 
        df_final = pd.concat([df_final, df_int], ignore_index=True)
        df_final.to_csv("captions.csv", mode='a', index=False, header=False)