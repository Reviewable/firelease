const {createHash, hash, randomUUID} = require('node:crypto');
const {performance} = require('node:perf_hooks');
const id = randomUUID();
const keys = Array.from({length: 1000}, (_, i) => `owner|repository-${i % 31}|${i}`);
const implementations = {
  createHash: text => createHash('sha256').update(text).digest().readUIntBE(0, 6),
  oneShotSha256: text => hash('sha256', text, 'buffer').readUIntBE(0, 6),
  fnv1aWithAvalanche: text => {
    let value = 2166136261;
    for (let i = 0; i < text.length; i++) value = Math.imul(value ^ text.charCodeAt(i), 16777619);
    value ^= value >>> 16;
    value = Math.imul(value, 0x85ebca6b);
    value ^= value >>> 13;
    value = Math.imul(value, 0xc2b2ae35);
    return (value ^ value >>> 16) >>> 0;
  }
};
let checksum = 0;
const iterations = 200000;
const samples = Object.fromEntries(Object.keys(implementations).map(name => [name, []]));
for (let round = 0; round < 6; round++) {
  for (const [name, score] of Object.entries(implementations)) {
    const start = performance.now();
    for (let i = 0; i < iterations; i++) checksum += score(JSON.stringify([id, keys[i % keys.length]]));
    if (round) samples[name].push((performance.now() - start) * 1e6 / iterations);
  }
}
console.log(JSON.stringify({node: process.version, iterationsPerSample: iterations,
  samplesNsPerScore: samples, checksumFinite: Number.isFinite(checksum)}, null, 2));
