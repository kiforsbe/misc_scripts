// Tests for the pure, DOM/network-independent functions in
// youtube-video-downloader.user-script.js.
//
// The userscript is a single Tampermonkey file (by design - Tampermonkey
// scripts are simplest as one file), so it isn't a Node module by default.
// To test it without duplicating logic into a second file, the script ends
// with a `if (typeof module !== 'undefined' && module.exports) { ... }`
// guard that exports its pure functions - a no-op in the browser/GM
// sandbox, where `module` is never defined. This harness loads the real
// script file via vm.runInNewContext with minimal DOM/GM stubs (just
// enough that the script's top-level setup code doesn't throw - none of
// the stubs are exercised by the functions under test here) and then
// tests the exported pure functions directly.
//
// Run with: node --test youtube-video-downloader/tests/test_userscript_pure.js

const { test, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const SCRIPT_PATH = path.join(__dirname, '..', 'youtube-video-downloader.user-script.js');

function loadUserscriptExports() {
  // Runs the script in THIS realm (via vm.runInThisContext) rather than an
  // isolated vm.createContext sandbox, so the plain objects/arrays the
  // exported functions return share this process's real Object/Array
  // prototypes - deepStrictEqual compares prototype identity, which a
  // separate vm realm would otherwise fail even for structurally-identical
  // values. Browser/GM globals the script touches are patched onto the
  // real `global` object. The exported functions are closures that read
  // globals like `window`/`document` at CALL time, not just at load time
  // (e.g. extractVideoIdFromAnyUrl reads window.location.origin on every
  // call) - so the stubs must stay installed for the lifetime of the test
  // run, not just during the initial script load. Cleanup is deferred to
  // an `after()` hook instead of a `finally` here.
  const source = fs.readFileSync(SCRIPT_PATH, 'utf8');
  const sandboxModule = { exports: {} };

  const noop = () => {};
  const stubs = {
    module: sandboxModule,
    exports: sandboxModule.exports,
    GM_addStyle: noop,
    GM_xmlhttpRequest: noop,
    MutationObserver: class {
      constructor(callback) { this.callback = callback; }
      observe() {}
      disconnect() {}
    },
    localStorage: { getItem: () => null, setItem: noop },
    navigator: {},
    window: {
      addEventListener: noop,
      removeEventListener: noop,
      innerWidth: 1920,
      innerHeight: 1080,
      location: {
        pathname: '/', // deliberately not a watch page, so init code skips DOM insertion
        search: '',
        href: 'https://www.youtube.com/',
        origin: 'https://www.youtube.com'
      }
    },
    document: {
      addEventListener: noop,
      removeEventListener: noop,
      title: '',
      body: {},
      documentElement: {},
      createElement: () => ({
        style: {},
        classList: { add: noop, remove: noop, contains: () => false },
        appendChild: noop,
        removeChild: noop,
        setAttribute: noop,
        getAttribute: () => null,
        addEventListener: noop
      }),
      getElementById: () => null,
      querySelector: () => null,
      querySelectorAll: () => []
    }
  };

  const keys = Object.keys(stubs);
  const previous = {};
  for (const key of keys) {
    previous[key] = global[key];
    global[key] = stubs[key];
  }

  after(() => {
    for (const key of keys) {
      if (previous[key] === undefined) {
        delete global[key];
      } else {
        global[key] = previous[key];
      }
    }
  });

  vm.runInThisContext(source, { filename: SCRIPT_PATH });

  assert.ok(
    sandboxModule.exports && Object.keys(sandboxModule.exports).length > 0,
    'userscript did not export anything - check the module.exports guard at the end of the file'
  );
  return sandboxModule.exports;
}

const lib = loadUserscriptExports();

// --- isValidFormatId ---

test('isValidFormatId accepts real format ids', () => {
  assert.equal(lib.isValidFormatId('137'), true);
  assert.equal(lib.isValidFormatId(137), true);
  assert.equal(lib.isValidFormatId('251-drc'), true);
});

test('isValidFormatId rejects null/undefined/none-ish values', () => {
  assert.equal(lib.isValidFormatId(null), false);
  assert.equal(lib.isValidFormatId(undefined), false);
  assert.equal(lib.isValidFormatId('null'), false);
  assert.equal(lib.isValidFormatId('none'), false);
  assert.equal(lib.isValidFormatId('N/A'), false);
  assert.equal(lib.isValidFormatId(''), false);
  assert.equal(lib.isValidFormatId('   '), false);
});

// --- gcd / formatBitrate / formatVideoDetails ---

test('gcd computes greatest common divisor', () => {
  assert.equal(lib.gcd(1920, 1080), 120);
  assert.equal(lib.gcd(16, 9), 1);
  assert.equal(lib.gcd(9, 0), 9);
});

test('formatBitrate renders kbps or empty string', () => {
  assert.equal(lib.formatBitrate(128), '128k');
  assert.equal(lib.formatBitrate(160.4), '160k');
  assert.equal(lib.formatBitrate(0), '');
  assert.equal(lib.formatBitrate(null), '');
  assert.equal(lib.formatBitrate(undefined), '');
});

test('formatVideoDetails combines aspect ratio and resolution', () => {
  assert.equal(lib.formatVideoDetails({ width: 1920, height: 1080, fps: 60 }), '[16:9] 1080p60');
  assert.equal(lib.formatVideoDetails({ height: 720 }), '[?:?] 720p');
  assert.equal(lib.formatVideoDetails({}), '');
});

// --- extractVideoIdFromAnyUrl / isVideoUrlJS ---

test('extractVideoIdFromAnyUrl parses watch, youtu.be, and embed URLs', () => {
  assert.equal(lib.extractVideoIdFromAnyUrl('https://www.youtube.com/watch?v=abc123XYZ_-'), 'abc123XYZ_-');
  assert.equal(lib.extractVideoIdFromAnyUrl('https://youtu.be/abc123XYZ_-'), 'abc123XYZ_-');
  assert.equal(lib.extractVideoIdFromAnyUrl('https://www.youtube.com/embed/abc123XYZ_-'), 'abc123XYZ_-');
  assert.equal(lib.extractVideoIdFromAnyUrl('https://www.youtube.com/embed/abc123XYZ_-?autoplay=1'), 'abc123XYZ_-');
});

test('extractVideoIdFromAnyUrl returns null for non-video URLs', () => {
  assert.equal(lib.extractVideoIdFromAnyUrl('https://www.youtube.com/'), null);
  assert.equal(lib.extractVideoIdFromAnyUrl('https://www.youtube.com/results?search_query=x'), null);
  assert.equal(lib.extractVideoIdFromAnyUrl('not a url'), null);
});

test('isVideoUrlJS matches the same URL shapes as extractVideoIdFromAnyUrl', () => {
  assert.equal(lib.isVideoUrlJS('https://www.youtube.com/watch?v=abc123'), true);
  assert.equal(lib.isVideoUrlJS('https://youtu.be/abc123'), true);
  assert.equal(lib.isVideoUrlJS('https://www.youtube.com/'), false);
});

// --- sanitizeFormatPayload / getValidAudioFormats ---

test('sanitizeFormatPayload drops formats with acodec/vcodec "none" and invalid ids', () => {
  const raw = {
    url: 'https://www.youtube.com/watch?v=vid1',
    audio_formats: [
      { format_id: '140', acodec: 'aac' },
      { format_id: '160', acodec: 'none' }, // video-only, dropped from audio list
      { format_id: null, acodec: 'opus' } // invalid id, dropped
    ],
    video_formats: [
      { format_id: '137', vcodec: 'avc1' },
      { format_id: '139', vcodec: 'none' } // audio-only, dropped from video list
    ]
  };
  const result = lib.sanitizeFormatPayload(raw, 'vid1');
  assert.equal(result.ok, true);
  assert.deepEqual(result.data.audio_formats.map((f) => f.format_id), ['140']);
  assert.deepEqual(result.data.video_formats.map((f) => f.format_id), ['137']);
  assert.equal(result.stats.droppedAudioCount, 2);
  assert.equal(result.stats.droppedVideoCount, 1);
});

test('sanitizeFormatPayload rejects a payload for a different video id (stale response)', () => {
  const raw = {
    url: 'https://www.youtube.com/watch?v=other-video',
    audio_formats: [{ format_id: '140', acodec: 'aac' }],
    video_formats: []
  };
  const result = lib.sanitizeFormatPayload(raw, 'expected-video');
  assert.equal(result.ok, false);
  assert.match(result.error, /mismatch/i);
});

test('sanitizeFormatPayload rejects non-object payloads', () => {
  assert.equal(lib.sanitizeFormatPayload(null).ok, false);
  assert.equal(lib.sanitizeFormatPayload('nope').ok, false);
});

test('getValidAudioFormats filters and ignores stale-video payloads', () => {
  const data = {
    url: 'https://www.youtube.com/watch?v=vid1',
    audio_formats: [
      { format_id: '140', acodec: 'aac' },
      { format_id: '141', acodec: 'none' }
    ]
  };
  assert.deepEqual(
    lib.getValidAudioFormats(data, 'vid1').map((f) => f.format_id),
    ['140']
  );
  assert.deepEqual(lib.getValidAudioFormats(data, 'a-different-video'), []);
});

// --- buildDownloadStartParams ---

test('buildDownloadStartParams only includes provided/truthy fields', () => {
  assert.deepEqual(
    lib.buildDownloadStartParams('https://x/y', null, null, null, null, null),
    { url: 'https://x/y' }
  );
  assert.deepEqual(
    lib.buildDownloadStartParams('https://x/y', '140', '137', 'mp4', '192k', null),
    {
      url: 'https://x/y',
      audio_format_id: '140',
      video_format_id: '137',
      target_format: 'mp4',
      target_audio_params: '192k'
    }
  );
});

test('buildDownloadStartParams includes title_hint only when provided', () => {
  assert.deepEqual(
    lib.buildDownloadStartParams('https://x/y', null, null, null, null, null, 'My Video Title'),
    { url: 'https://x/y', title_hint: 'My Video Title' }
  );
  assert.deepEqual(
    lib.buildDownloadStartParams('https://x/y', null, null, null, null, null, ''),
    { url: 'https://x/y' }
  );
});

test('buildDownloadStartParams includes split_chapters only when requested', () => {
  assert.deepEqual(
    lib.buildDownloadStartParams('https://x/y', '140', null, 'mp3', null, null, null, true),
    { url: 'https://x/y', audio_format_id: '140', target_format: 'mp3', split_chapters: 1 }
  );
  assert.deepEqual(
    lib.buildDownloadStartParams('https://x/y', '140', null, 'mp3', null, null, null, false),
    { url: 'https://x/y', audio_format_id: '140', target_format: 'mp3' }
  );
});

// --- countSplittableChapters ---

test('countSplittableChapters counts non-empty chapters when there are at least 2', () => {
  const chapters = [
    { title: 'A - One', start_time: 0, end_time: 60 },
    { title: 'Empty', start_time: 60, end_time: 60 },
    { title: 'B - Two', start_time: 60, end_time: 120 },
    { title: 'C - Three', start_time: 120, end_time: 180 }
  ];
  assert.equal(lib.countSplittableChapters({ chapters }), 3);
});

test('countSplittableChapters is 0 when there is nothing worth splitting', () => {
  assert.equal(lib.countSplittableChapters({ chapters: [{ title: 'Only', start_time: 0, end_time: 60 }] }), 0);
  assert.equal(lib.countSplittableChapters({ chapters: [] }), 0);
  assert.equal(lib.countSplittableChapters({}), 0);
  assert.equal(lib.countSplittableChapters(null), 0);
  assert.equal(lib.countSplittableChapters({ chapters: 'nope' }), 0);
});

// --- shouldOfferTrackSplit ---

test('shouldOfferTrackSplit offers chaptered videos and long videos', () => {
  const chapters = [{ start_time: 0, end_time: 60 }, { start_time: 60, end_time: 120 }];
  assert.equal(lib.shouldOfferTrackSplit({ chapters, duration: 120 }), true);
  assert.equal(lib.shouldOfferTrackSplit({ chapters: [], duration: 600 }), true);
  assert.equal(lib.shouldOfferTrackSplit({ duration: 599 }), false);
  assert.equal(lib.shouldOfferTrackSplit({}), false);
  assert.equal(lib.shouldOfferTrackSplit(null), false);
});

// --- buildFallbackFilename ---

test('buildFallbackFilename picks an extension from the request', () => {
  assert.equal(lib.buildFallbackFilename('Song', 'mp3', null, false), 'Song.mp3');
  assert.equal(lib.buildFallbackFilename('Clip', null, '137', false), 'Clip.mp4');
  assert.equal(lib.buildFallbackFilename('Song', null, null, false), 'Song.mp3');
});

test('buildFallbackFilename uses .zip for chapter-split downloads', () => {
  assert.equal(lib.buildFallbackFilename('Mix', 'mp3', null, true), 'Mix.zip');
});

// --- parseContentDispositionFilename ---

test('parseContentDispositionFilename extracts a quoted filename', () => {
  assert.equal(
    lib.parseContentDispositionFilename('attachment; filename="My Video.mp4"', 'fallback.mp4'),
    'My Video.mp4'
  );
});

test('parseContentDispositionFilename decodes filename* UTF-8 encoding', () => {
  assert.equal(
    lib.parseContentDispositionFilename(
      "attachment; filename*=UTF-8''My%20Video%20%E2%9C%93.mp4",
      'fallback.mp4'
    ),
    'My Video ✓.mp4'
  );
});

test('parseContentDispositionFilename falls back when header is absent/unparseable', () => {
  assert.equal(lib.parseContentDispositionFilename('', 'fallback.mp4'), 'fallback.mp4');
  assert.equal(lib.parseContentDispositionFilename('not a content-disposition value', 'fallback.mp4'), 'fallback.mp4');
  assert.equal(lib.parseContentDispositionFilename(null, 'fallback.mp4'), 'fallback.mp4');
});

// --- decideJobStatusAction ---

test('decideJobStatusAction maps each server status to the right action', () => {
  assert.deepEqual(lib.decideJobStatusAction({ status: 'complete' }), { action: 'fetch_result' });

  assert.deepEqual(
    lib.decideJobStatusAction({ status: 'error', error: 'boom' }),
    { action: 'error', message: 'boom' }
  );
  assert.equal(lib.decideJobStatusAction({ status: 'error', error: null }).message, 'Download failed');

  assert.deepEqual(
    lib.decideJobStatusAction({ status: 'cancelled', error: 'Cancelled by user' }),
    { action: 'cancelled', message: 'Cancelled by user' }
  );

  assert.equal(lib.decideJobStatusAction({ status: 'dropped' }).action, 'dropped');

  const downloading = lib.decideJobStatusAction({ status: 'downloading', percent: 42, message: 'at 42%' });
  assert.deepEqual(downloading, { action: 'continue_polling', percent: 42, message: 'at 42%' });
});

test('decideJobStatusAction treats malformed/empty responses as a transient hiccup', () => {
  assert.deepEqual(lib.decideJobStatusAction(null), { action: 'retry_poll' });
  assert.deepEqual(lib.decideJobStatusAction({}), { action: 'retry_poll' });
  assert.deepEqual(lib.decideJobStatusAction('not an object'), { action: 'retry_poll' });
});

test('decideJobStatusAction defaults percent/message for an unrecognized in-progress status', () => {
  const result = lib.decideJobStatusAction({ status: 'queued' });
  assert.equal(result.action, 'continue_polling');
  assert.equal(result.percent, 0);
  assert.equal(result.message, 'Downloading...');
});
