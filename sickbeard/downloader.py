'''
Created on Apr 24, 2013

@author: Dermot Buckley, dermot@buckley.ie
'''

import time
import os.path
import pickle

from sickbeard import logger
from sickbeard import version
from sickbeard.exceptions import ex
from sickbeard.helpers import isMediaFile
from sickbeard import postProcessor
from sickbeard import exceptions
import sickbeard

LIBTORRENT_AVAILABLE = False

try:
    #http://www.rasterbar.com/products/libtorrent/manual.html
    import libtorrent as lt
    logger.log('libtorrent import succeeded, libtorrent is available', logger.MESSAGE)
    LIBTORRENT_AVAILABLE = True
except ImportError:
    logger.log('libtorrent import failed, functionality will not be available', logger.MESSAGE)
    
# the number of seconds we wait after adding a torrent to see signs of download beginning
TORRENT_START_WAIT_TIMEOUT_SECS = 90

# The actual running lt session.  Obtain it by calling _get_session() - which
# will create it if needed.
_lt_sess = None

# a list of running torrents, each entry being a dict with torrent properties.
running_torrents = []



def download_from_torrent(torrent, postProcessingDone=False):
    """
    Download the files from a magnet link or torrent url.
    Returns True if the download begins, and forks off a thread to complete the download.
    Note: This function will block until the download gives some indication that it
    has started correctly (or TORRENT_START_WAIT_TIMEOUT_SECS is reached).
    
    @param torrent: (string) url (http or https) to a torrent file, a raw torrent file, or a magnet link.
    @param postProcessingDone: (bool) If true, the torrent will be flagged as "already post processed".
    @return: (bool) True if the download *starts*, False otherwise.
    """

    global running_torrents
    try:
        sess = _get_session()
        atp = {}    # add_torrent_params
        atp["save_path"] = _get_save_path(True)
        atp["storage_mode"] = lt.storage_mode_t.storage_mode_sparse
        atp["paused"] = False
        atp["auto_managed"] = True
        atp["duplicate_is_error"] = True
        have_torrentFile = False
        if torrent.startswith('magnet:') or torrent.startswith('http://') or torrent.startswith('https://'):
            logger.log(u'Adding torrent to session: {0}'.format(torrent), logger.DEBUG)
            atp["url"] = torrent
        else:
            e = lt.bdecode(torrent)
            info = lt.torrent_info(e)
            logger.log(u'Adding torrent to session: {0}'.format(info.name()), logger.DEBUG)
            have_torrentFile = True
                
            try:
                atp["resume_data"] = open(os.path.join(atp["save_path"], info.name() + '.fastresume'), 'rb').read()
            except:
                pass
    
            atp["ti"] = info
        
        start_time = time.time()
        h = sess.add_torrent(atp)
    
        #handles.append(h)
        running_torrents.append({
            'torrent': torrent,
            'handle': h,
            'post_processed': postProcessingDone,
            'have_torrentFile': have_torrentFile
        })
    
        h.set_max_connections(400)
        h.set_max_uploads(-1)
        
        startedDownload = False
        while not startedDownload:
            time.sleep(0.5)
            if h.has_metadata():
                s = h.status(0x0) # 0x0 because we don't want any of the optional info
                
                if s.state in [lt.torrent_status.seeding, 
                               lt.torrent_status.downloading,
                               lt.torrent_status.finished, 
                               lt.torrent_status.downloading_metadata]:
                    name = h.get_torrent_info().name()
                    logger.log(u'Torrent "{0}" has state "{1}" ({2}), interpreting as downloading'.format(name, s.state, repr(s.state)), 
                               logger.MESSAGE)
                    return True
            else:
                # no metadata?  Definitely not started yet then!
                pass
            
            # check for timeout
            if time.time() - start_time > TORRENT_START_WAIT_TIMEOUT_SECS:
                logger.log(u'Torrent has failed to start within timeout {0}secs.  Removing'.format(TORRENT_START_WAIT_TIMEOUT_SECS),
                           logger.WARNING)
                _remove_torrent_by_handle(h)
                return False
                
    except Exception, e:
        logger.log('Error trying to download via libtorrent: ' + ex(e), logger.ERROR)
        return False
    
def set_max_dl_speed(max_dl_speed):
    """
    Set the download rate limit for libtorrent if it's running
    @param max_dl_speed: integer.  Rate in kB/s 
    """
    sess = _get_session(False)
    if sess:
        _lt_sess.set_download_rate_limit(max_dl_speed * 1024)

def set_max_ul_speed(max_ul_speed):
    """
    Set the upload rate limit for libtorrent if it's running
    @param max_ul_speed: integer.  Rate in kB/s 
    """
    sess = _get_session(False)
    if sess:
        _lt_sess.set_upload_rate_limit(max_ul_speed * 1024)
    
def _get_session(createIfNeeded=True):
    global _lt_sess
    if _lt_sess is None and createIfNeeded:
        _lt_sess = lt.session()
        _lt_sess.set_download_rate_limit(sickbeard.LIBTORRENT_MAX_DL_SPEED * 1024)
        _lt_sess.set_upload_rate_limit(sickbeard.LIBTORRENT_MAX_UL_SPEED * 1024)
        
        settings = lt.session_settings()
        settings.user_agent = 'sickbeard_bricky-{0}/{1}'.format(version.SICKBEARD_VERSION.replace(' ', '-'), lt.version)
        
        _lt_sess.listen_on(6881, 6891)
        _lt_sess.set_settings(settings)
        _lt_sess.set_alert_mask(lt.alert.category_t.error_notification |
                                #lt.alert.category_t.port_mapping_notification |
                                lt.alert.category_t.storage_notification |
                                #lt.alert.category_t.tracker_notification |
                                lt.alert.category_t.status_notification |
                                lt.alert.category_t.performance_warning)
        
    return _lt_sess

def _get_save_path(ensureExists=False):
    """
    Get the save path for torrent data
    """
    pth = os.path.join(sickbeard.LIBTORRENT_WORKING_DIR, 'data')
    if ensureExists and not os.path.exists(pth):
            os.makedirs(pth)
    return pth

def _get_running_path(ensureExists=False):
    """
    Get the save path for running torrent info
    """
    pth = os.path.join(sickbeard.LIBTORRENT_WORKING_DIR, 'running')
    if ensureExists and not os.path.exists(pth):
            os.makedirs(pth)
    return pth

# def add_suffix(val):
#     prefix = ['B', 'kB', 'MB', 'GB', 'TB']
#     for i in range(len(prefix)):
#         if abs(val) < 1000:
#             if i == 0:
#                 return '%5.3g%s' % (val, prefix[i])
#             else:
#                 return '%4.3g%s' % (val, prefix[i])
#         val /= 1000
# 
#     return '%6.3gPB' % val

def _remove_torrent_by_handle(h):
    global running_torrents
    sess = _get_session(False)
    if sess:
        theEntry = next(d for d in running_torrents if d['handle'] == h)
        running_torrents.remove(theEntry)
        try:
            fr_file = os.path.join(_get_save_path(),
                                   theEntry['handle'].get_torrent_info().name() + '.fastresume')
            os.remove(fr_file)
        except Exception:
            pass
        sess.remove_torrent(theEntry['handle'], 1)
        
def _get_running_torrents_pickle_path(createDirsIfNeeded=False):
    torrent_save_dir = _get_running_path(createDirsIfNeeded)
    return os.path.join(torrent_save_dir, 'running_torrents.pickle')
        
def _load_saved_torrents(deleteSaveFile=True):
    torrent_save_file = _get_running_torrents_pickle_path(False)
    if os.path.isfile(torrent_save_file):
        data_from_pickle = pickle.load(open(torrent_save_file, "rb"))
        for td in data_from_pickle:
            download_from_torrent(td['torrent'], td['post_processed'])
        if deleteSaveFile:
            os.remove(torrent_save_file)
    
def _save_running_torrents():
    global running_torrents
    if len(running_torrents):
        data_to_pickle = []
        for torrent_data in running_torrents:
            data_to_pickle.append({
                'torrent': torrent_data['torrent'],
                'post_processed': torrent_data['post_processed']
            })
        torrent_save_file = _get_running_torrents_pickle_path(True)
        logger.log(u'Saving running torrents to "{0}"'.format(torrent_save_file), logger.DEBUG)
        pickle.dump(data_to_pickle, open(torrent_save_file, "wb"))


class TorrentProcessHandler():
    def __init__(self):
        self.shutDownImmediate = False
        self.loadedRunningTorrents = False
        self.amActive = False # just here to keep the scheduler class happy!
    
    def run(self):
        """
        Called every few seconds to handle any running/finished torrents
        """
        
        if not LIBTORRENT_AVAILABLE:
            return
        
        if not self.loadedRunningTorrents:
            torrent_save_file = _get_running_torrents_pickle_path(False)
            if os.path.isfile(torrent_save_file):
                logger.log(u'Saved torrents found in {0}, loading'.format(torrent_save_file), logger.DEBUG)
                _load_saved_torrents()
            
            self.loadedRunningTorrents = True    

        sess = _get_session(False)
        if sess is not None:
            while 1:
                a = sess.pop_alert()
                if not a: break
                
                if type(a) == str:
                    logger.log(u'{0}'.format(a), logger.DEBUG)
                else:
                    logger.log(u'({0}): {1}'.format(type(a).__name__, a.message()), logger.DEBUG)
                
            for torrent_data in running_torrents:
                if torrent_data['handle'].has_metadata():
                    name = torrent_data['handle'].get_torrent_info().name()
                    
                    if not torrent_data['have_torrentFile']:
                        # if this was a magnet or url, and we now have downloaded the metadata
                        # for it, best to save it locally in case we need to resume
                        ti = torrent_data['handle'].get_torrent_info()
                        torrentFile = lt.create_torrent(ti)
                        torrent_data['torrent'] = lt.bencode(torrentFile.generate())
                        torrent_data['have_torrentFile'] = True
                        logger.log(u'Created torrent file for {0} as metadata d/l is now complete'.format(name), logger.DEBUG)

                else:
                    name = '-'
                    
                s = torrent_data['handle'].status()
                
                if s.state in [lt.torrent_status.seeding,
                               lt.torrent_status.finished]:
                    if not torrent_data['post_processed']:
                        # torrent has just completed download, so we need to do
                        # post-processing on it.
                        torrent_data['post_processed'] = True
                        ti = torrent_data['handle'].get_torrent_info()
                        any_file_success = False
                        for f in ti.files():
                            fullpath = os.path.join(sickbeard.LIBTORRENT_WORKING_DIR, 'data', f.path)
                            logger.log(u'Post-processing "{0}"'.format(fullpath), logger.DEBUG)
                            if isMediaFile(fullpath):
                                logger.log(u'this is a media file', logger.DEBUG)
                                try:
                                    processor = postProcessor.PostProcessor(fullpath)
                                    if processor.process():
                                        logger.log(u'Success post-processing "{0}"'.format(fullpath), logger.DEBUG)
                                        any_file_success = True
                                except exceptions.PostProcessingFailed, e:
                                    logger.log(u'Failed post-processing file "{0}" with error "{1}"'.format(fullpath, ex(e)), 
                                               logger.ERROR)
                                    
                        if not any_file_success:
                            logger.log(u'When post-processing the completed torrent {0}, no useful files were found.'.format(name), logger.ERROR)
                    else:
                        # post-processing has already been performed.  So we just 
                        # need to ensure check the ratio and delete the torrent
                        # if we're good.
                        currentRatio = 0.0 if s.total_download == 0 else float(s.total_upload)/float(s.total_download)
                        if currentRatio >=  sickbeard.LIBTORRENT_SEED_TO_RATIO:
                            logger.log(u'Torrent "{0}" has seeded to ratio {1}.  Removing it.'.format(name, currentRatio), logger.MESSAGE)
                            _remove_torrent_by_handle(torrent_data['handle'])
                        else:
                            logger.log(u'"{0}" seeding {1:.3f}'.format(name, currentRatio), logger.DEBUG)
                elif s.state == lt.torrent_status.downloading:
                    logger.log(u'"{0}" downloading {1:.2f}%'.format(name, s.progress * 100.0), logger.DEBUG)
                        
            if self.shutDownImmediate:
                # there's an immediate shutdown waiting to happen, save any running torrents
                # and get ready to stop
                logger.log(u"Torrent shutdown immediate", logger.DEBUG)
                sess.pause()
                for torrent_data in running_torrents:
                    h = torrent_data['handle']
                    if not h.is_valid() or not h.has_metadata():
                        continue
                    data = lt.bencode(torrent_data['handle'].write_resume_data())
                    save_path = _get_save_path(True)
                    tname = h.get_torrent_info().name()
                    logger.log(u'Saving fastresume data for "{0}"'.format(tname), logger.DEBUG)
                    open(os.path.join(save_path, tname + '.fastresume'), 'wb').write(data)
                
                _save_running_torrents()
                
                
    
#     
# Robbed, almost verbatim, from 
# http://code.google.com/p/torrenter/source/browse/plugin.video.torrenter/Downloader.py
# 
# class Torrent:
#     torrentFile = None
#     magnetLink = None
#     storageDirectory = ''
#     torrentFilesDirectory = 'torrents'
#     startPart = 0
#     endPart = 0
#     partOffset = 0
#     torrentHandle = None
#     session = None
#     downloadThread = None
#     threadComplete = False
# 
#     def __init__(self, storageDirectory = '', torrentFile = '', torrentFilesDirectory = 'torrents'):
#         self.torrentFilesDirectory = torrentFilesDirectory
#         self.storageDirectory = storageDirectory
#         if not os.path.exists(self.storageDirectory + os.sep + self.torrentFilesDirectory):
#             os.makedirs(self.storageDirectory + os.sep + self.torrentFilesDirectory)
#         if os.path.exists(torrentFile):
#             self.torrentFile = torrentFile
#             self.torrentFileInfo = libtorrent.torrent_info(self.torrentFile)
#         elif re.match("^magnet\:.+$", torrentFile):
#             self.magnetLink = torrentFile
#        
#     def saveTorrent(self, torrentUrl):
#         if re.match("^magnet\:.+$", torrentUrl):
#             self.magnetLink = torrentUrl
#             self.magnetToTorrent(torrentUrl)
#             return self.magnetLink
#         else:
#             torrentFile = self.storageDirectory + os.sep + self.torrentFilesDirectory + os.sep + self.md5(torrentUrl) + '.torrent'
#             try:
#                 request = urllib2.Request(torrentUrl)
#                 request.add_header('Referer', torrentUrl)
#                 localFile = open(torrentFile, "w+b")
#                 result = urllib2.urlopen(request)
#                 localFile.write(result.read())
#                 localFile.close()
#             except:
#                 logger.log(u'Unable to save torrent file from "{0}" to "{1}" in Torrent::saveTorrent'.format(torrentUrl, torrentFile), logger.ERROR)
#                 return
#             if os.path.exists(torrentFile):
#                 self.torrentFileInfo = libtorrent.torrent_info(torrentFile)
#                 baseName = os.path.basename(self.getFilePath())
#                 newFile = self.storageDirectory + os.sep + self.torrentFilesDirectory + os.sep + baseName + '.' + self.md5(torrentUrl) + '.torrent'
#                 if not os.path.exists(newFile):                
#                     try:
#                         os.rename(torrentFile, newFile)
#                     except:
#                         logger.log(u'Unable to rename torrent file from "{0}" to "{1}" in Torrent::renameTorrent'.format(torrentFile, newFile), logger.ERROR)
#                         return
#                 self.torrentFile = newFile
#                 self.torrentFileInfo = libtorrent.torrent_info(self.torrentFile)
#                 return self.torrentFile
# 
#     def getMagnetInfo(self):
#         magnetSettings = {
#             'save_path': self.storageDirectory,
#             'storage_mode': libtorrent.storage_mode_t(2),
#             'paused': True,
#             'auto_managed': True,
#             'duplicate_is_error': True
#         }
# 
#         logger.log(u"Magnet link is converting", logger.DEBUG)
#         self.torrentHandle = libtorrent.add_magnet_uri(self.session, self.magnetLink, magnetSettings)
#         iterator = 0
#         while not self.torrentHandle.has_metadata():
#             time.sleep(0.1)
#             #progressBar.update(iterator)
#             iterator += 1
#             if iterator == 100:
#                 iterator = 0
#             #if progressBar.iscanceled():
#             #    progressBar.update(0)
#             #    progressBar.close()
#             #    return
#         #progressBar.update(0)
#         #progressBar.close()
#         return self.torrentHandle.get_torrent_info()
# 
#     def magnetToTorrent(self, magnet):
#         self.magnetLink = magnet
#         self.initSession()
#         torrentInfo = self.getMagnetInfo()
#         try:
#             torrentFile = libtorrent.create_torrent(torrentInfo)
#             baseName = os.path.basename(self.storageDirectory + os.sep + torrentInfo.files()[0].path).decode('utf-8').encode('ascii', 'ignore')
#             self.torrentFile = self.storageDirectory + os.sep + self.torrentFilesDirectory + os.sep + baseName + '.torrent'
#             torentFileHandler = open(self.torrentFile, "wb")
#             torentFileHandler.write(libtorrent.bencode(torrentFile.generate()))
#             torentFileHandler.close()
#             self.torrentFileInfo = libtorrent.torrent_info(self.torrentFile)
#         except:
#             logger.log(u'Your library is out of date and can\'t save magnet-links.', logger.ERROR)
#             self.torrentFileInfo = torrentInfo
# 
#     def getUploadRate(self):
#         if None == self.torrentHandle:
#             return 0
#         else:
#             return self.torrentHandle.status().upload_payload_rate
# 
#     def getDownloadRate(self):
#         if None == self.torrentHandle:
#             return 0
#         else:
#             return self.torrentHandle.status().download_payload_rate
# 
#     def getPeers(self):
#         if None == self.torrentHandle:
#             return 0
#         else:
#             return self.torrentHandle.status().num_peers
# 
#     def getSeeds(self):
#         if None == self.torrentHandle:
#             return 0
#         else:
#             return self.torrentHandle.status().num_seeds
# 
#     def getFileSize(self, contentId = 0):
#         return self.getContentList()[contentId].size
# 
#     def getFilePath(self, contentId = 0):
#         return self.storageDirectory + os.sep + self.getContentList()[contentId].path
# 
#     def getContentList(self):
#         return self.torrentFileInfo.files()
# 
#     def setUploadLimit(self, bytesPerSecond):
#         self.session.set_upload_rate_limit(int(bytesPerSecond))
# 
#     def setDownloadLimit(self, bytesPerSecond):
#         self.session.set_download_rate_limit(int(bytesPerSecond))
# 
#     def md5(self, string):
#         hasher = hashlib.md5()
#         hasher.update(string)
#         return hasher.hexdigest()
# 
#     def downloadProcess(self, contentId):
#         for part in range(self.startPart, self.endPart + 1):
#             self.getPiece(part)
#             time.sleep(0.1)
#             self.checkThread()
#         self.threadComplete = True
# 
#     def initSession(self):
#         try:
#             self.session.remove_torrent(self.torrentHandle)
#         except:
#             pass
#         self.session = libtorrent.session()
#         self.session.start_dht()
#         self.session.add_dht_router("router.bittorrent.com", 6881)
#         self.session.add_dht_router("router.utorrent.com", 6881)
#         self.session.add_dht_router("router.bitcomet.com", 6881)
#         self.session.listen_on(6881, 6891)
#         self.session.set_alert_mask(libtorrent.alert.category_t.storage_notification)
# 
#     def startSession(self, contentId = 0, seeding = True):
#         self.initSession()
#         if None == self.magnetLink:
#             self.torrentHandle = self.session.add_torrent({'ti': self.torrentFileInfo, 'save_path': self.storageDirectory})
#         else:
#             self.torrentFileInfo = self.getMagnetInfo()
# 
#         selectedFileInfo = self.getContentList()[contentId]
#         self.partOffset = 50 * 1024 * 1024 / self.torrentFileInfo.piece_length()#50 MB
#         #print 'partOffset ' + str(self.partOffset)
#         self.startPart = selectedFileInfo.offset / self.torrentFileInfo.piece_length()
#         self.endPart = (selectedFileInfo.offset + selectedFileInfo.size) / self.torrentFileInfo.piece_length()
# 
#         for i in range(self.torrentFileInfo.num_pieces()):
#             self.torrentHandle.piece_priority(i, 0)
#         for i in range(self.startPart, self.startPart + self.partOffset):
#             if i <= self.endPart:
#                 self.torrentHandle.piece_priority(i, 7)
#         self.torrentHandle.set_sequential_download(True)
#         thread.start_new_thread(self.downloadProcess, (contentId,))
#         if seeding:# and None == self.magnetLink:
#             thread.start_new_thread(self.addToSeeding, ())
# 
#     def addToSeeding(self):
#         for filename in os.listdir(self.storageDirectory + os.sep + self.torrentFilesDirectory):
#             currentFile = self.storageDirectory + os.sep + self.torrentFilesDirectory + os.sep + filename
#             if re.match('^.+\.torrent$', currentFile):
#                 info = libtorrent.torrent_info(currentFile)
#                 fileSettings = {
#                     'ti': info,
#                     'save_path': self.storageDirectory,
#                     'paused': False,
#                     'auto_managed': False,
#                     'seed_mode': True,
#                 }
#                 self.session.add_torrent(fileSettings)
# 
#     def fetchParts(self):
#         priorities = self.torrentHandle.piece_priorities()
#         status = self.torrentHandle.status()
#         downloading = 0
#         #print priorities
#         if len(status.pieces) == 0:
#             return
#         for part in range(self.startPart, self.endPart + 1):
#             if priorities[part] != 0 and status.pieces[part] == False:
#                 self.checkThread()
#                 downloading += 1
#         for part in range(self.startPart, self.endPart + 1):
#             if priorities[part] == 0 and downloading < self.partOffset:
#                 self.checkThread()
#                 self.torrentHandle.piece_priority(part, 1)
#                 downloading += 1
#         for part in range(self.startPart, self.endPart + 1):
#             if priorities[part] != 0 and status.pieces[part] == False:
#                 self.checkThread()
#                 break
# 
#     def checkThread(self):
#         if self.threadComplete == True:
#             self.session.remove_torrent(self.torrentHandle)
#             thread.exit()
# 
#     def getPiece(self, index):
#         cache = {}
#         if index in cache:
#             result = cache[index]
#             cache[index] = 0
#             return result
#         while True:
#             status = self.torrentHandle.status()
#             if len(status.pieces) == 0:
#                 break
#             if status.pieces[index] == True:
#                 break
#             time.sleep(0.5)
#             self.checkThread()
#         self.torrentHandle.read_piece(index)
#         while True:
#             part = self.session.pop_alert()
#             if isinstance(part, libtorrent.read_piece_alert):
#                 if part.piece == index:
#                     return part.buffer
#                 else:
#                     cache[part.piece] = part.buffer
#                 break
#             time.sleep(0.5)
#             self.checkThread()
