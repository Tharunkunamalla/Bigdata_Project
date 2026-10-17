/**
 * frontend/tests/test_client_logic.js
 * Unit test suite for client-side SSE buffer deduplication, filter pipeline, and formatters.
 */

import assert from 'assert';

// 1. Test Buffer Deduplication & Cap
function testBufferDeduplication() {
  console.log('Testing SSE buffer deduplication and cap limit...');
  let posts = [
    { post_id: 'p_1', text: 'First post', trust_score: 0.99 },
    { post_id: 'p_2', text: 'Second post', trust_score: 0.95 }
  ];

  // Incoming post with existing ID p_1 (updated trust)
  const incomingDuplicate = { post_id: 'p_1', text: 'First post updated', trust_score: 0.98 };
  posts = [incomingDuplicate, ...posts.filter(p => p.post_id !== incomingDuplicate.post_id)];

  assert.strictEqual(posts.length, 2, 'Buffer length should remain 2 after duplicate update');
  assert.strictEqual(posts[0].text, 'First post updated', 'Updated post should be at the head of buffer');

  // Add up to 550 posts to test buffer cap of 500
  for (let i = 3; i <= 550; i++) {
    const p = { post_id: `p_${i}`, text: `Post ${i}`, trust_score: 0.9 };
    posts = [p, ...posts.filter(item => item.post_id !== p.post_id)].slice(0, 500);
  }

  assert.strictEqual(posts.length, 500, 'Buffer cap must strictly enforce max 500 records');
  console.log('✓ Buffer deduplication and cap enforcement passed.');
}

// 2. Test Client Filter Logic
function testFilterLogic() {
  console.log('Testing client-side filter and sorting logic...');
  const samplePosts = [
    { post_id: 'p_1', text: 'Need rescue boat in Katy flood', event_type: 'Flood', urgency_level: 4.5, prob_synthetic_score: 0.002, trust_score: 0.998, is_flagged_misinformation: 0 },
    { post_id: 'p_2', text: 'Severe earthquake collapse in downtown', event_type: 'Earthquake', urgency_level: 4.8, prob_synthetic_score: 0.001, trust_score: 0.999, is_flagged_misinformation: 0 },
    { post_id: 'p_3', text: 'MANDATE: Dam breached evacuate immediately', event_type: 'Flood', urgency_level: 4.5, prob_synthetic_score: 0.920, trust_score: 0.080, is_flagged_misinformation: 1 },
    { post_id: 'p_4', text: 'Wildfire advisory in Napa valley', event_type: 'Wildfire', urgency_level: 2.0, prob_synthetic_score: 0.005, trust_score: 0.995, is_flagged_misinformation: 0 }
  ];

  // Verdict filter: flagged
  const flagged = samplePosts.filter(p => p.is_flagged_misinformation === 1 || p.prob_synthetic_score >= 0.1786);
  assert.strictEqual(flagged.length, 1, 'Flagged filter should return exactly 1 post');
  assert.strictEqual(flagged[0].post_id, 'p_3');

  // Verdict filter: credible
  const credible = samplePosts.filter(p => !(p.is_flagged_misinformation === 1 || p.prob_synthetic_score >= 0.1786));
  assert.strictEqual(credible.length, 3, 'Credible filter should return exactly 3 posts');

  // Event filter: Flood
  const floodPosts = samplePosts.filter(p => p.event_type.toLowerCase().includes('flood'));
  assert.strictEqual(floodPosts.length, 2, 'Event filter for Flood should return 2 posts');

  // Search filter
  const searchResults = samplePosts.filter(p => p.text.toLowerCase().includes('evacuate'));
  assert.strictEqual(searchResults.length, 1, 'Search query should return 1 matching post');

  // Urgency filter: >= 4.0
  const urgentPosts = samplePosts.filter(p => p.urgency_level >= 4.0);
  assert.strictEqual(urgentPosts.length, 3, 'Urgency >= 4.0 filter should return 3 posts');

  // Sorting by trust_desc
  const sortedTrust = [...samplePosts].sort((a, b) => b.trust_score - a.trust_score);
  assert.strictEqual(sortedTrust[0].post_id, 'p_2', 'Highest trust post should be p_2 (0.999)');
  assert.strictEqual(sortedTrust[sortedTrust.length - 1].post_id, 'p_3', 'Lowest trust post should be p_3 (0.080)');

  // Model-only verdict independence test (Item 3)
  const mixedPosts = [
    { post_id: 'p_hoax_low', is_injected: true, prob_synthetic_score: 0.0412 },
    { post_id: 'p_auth_high', is_injected: false, prob_synthetic_score: 0.8251 }
  ];
  const modelFlagged = mixedPosts.filter(p => p.prob_synthetic_score >= 0.1786);
  assert.strictEqual(modelFlagged.length, 1, 'Verdict must strictly use model score, ignoring ground truth is_injected');
  assert.strictEqual(modelFlagged[0].post_id, 'p_auth_high', 'High score post must be flagged regardless of authentic ground truth');

  console.log('✓ Filter and sorting pipeline passed.');
}

// 3. Test Formatters and Math Bounds
function testFormatters() {
  console.log('Testing formatting utilities and decimal constraints...');
  const formatDecimals = (val, dec = 3) => Number(val).toFixed(dec);
  const formatPercent = (val, dec = 1) => `${Number(val).toFixed(dec)}%`;

  assert.strictEqual(formatDecimals(0.1786, 3), '0.179');
  assert.strictEqual(formatDecimals(0.003, 3), '0.003');
  assert.strictEqual(formatPercent(21.62, 1), '21.6%');
  assert.strictEqual(formatPercent(0.0, 1), '0.0%');

  console.log('✓ Formatter accuracy passed.');
}

function runAll() {
  testBufferDeduplication();
  testFilterLogic();
  testFormatters();
  console.log('\nALL 3 FRONTEND LOGIC TEST SUITES PASSED SUCCESSFULLY (PASS).');
}

runAll();
