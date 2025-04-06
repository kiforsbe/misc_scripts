import logging
import time


class AVTransportService:
    """Handles media transport controls and playlist management"""
    def __init__(self):
        self.state = {
            'TransportState': 'STOPPED',  # PLAYING, PAUSED_PLAYBACK, STOPPED
            'CurrentURI': '',
            'CurrentTrack': 0,
            'NumberOfTracks': 0,
            'PlaybackSpeed': '1',
            'RelativeTimePosition': '00:00:00',
            'AbsoluteTimePosition': '00:00:00'
        }
        self.playlist = []
        self.current_media = None
        self._last_update = time.time()
        self.subscribers = set()

    def set_transport_uri(self, uri):
        """Set the URI of the media to be played"""
        self.state['CurrentURI'] = uri
        self.state['TransportState'] = 'STOPPED'
        self._notify_state_change()
        return True

    def play(self, speed='1'):
        """Start or resume playback"""
        if self.state['CurrentURI']:
            self.state['TransportState'] = 'PLAYING'
            self.state['PlaybackSpeed'] = speed
            self._notify_state_change()
            return True
        return False

    def pause(self):
        """Pause playback"""
        if self.state['TransportState'] == 'PLAYING':
            self.state['TransportState'] = 'PAUSED_PLAYBACK'
            self._notify_state_change()
            return True
        return False

    def stop(self):
        """Stop playback"""
        self.state['TransportState'] = 'STOPPED'
        self.state['RelativeTimePosition'] = '00:00:00'
        self._notify_state_change()
        return True

    def seek(self, target):
        """Seek to specific position"""
        if self.current_media:
            # Parse target format (time or track number)
            if ':' in target:  # Time format
                hours, minutes, seconds = map(int, target.split(':'))
                position_seconds = hours * 3600 + minutes * 60 + seconds
                self.state['RelativeTimePosition'] = target
                self._notify_state_change()
                return True
        return False

    def _notify_state_change(self):
        """Notify subscribers of state changes"""
        event_data = {
            'TransportState': self.state['TransportState'],
            'CurrentTrack': self.state['CurrentTrack'],
            'RelativeTime': self.state['RelativeTimePosition']
        }
        for subscriber in self.subscribers:
            try:
                self._send_event(subscriber, event_data)
            except Exception as e:
                self.subscribers.remove(subscriber)
                logging.error(f"Failed to notify subscriber {subscriber}: {e}")

    def _send_event(self, subscriber, data):
        """Send event notification to subscriber"""
        # Implementation will use HTTP NOTIFY
        pass
