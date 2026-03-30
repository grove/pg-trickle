import sys
STRIDE = 0x9e3779b97f4a7c15
MASK64 = (1 << 64) - 1
def next_u64(s):
    s = (s + STRIDE) & MASK64
    v = s
    v = ((v ^ (v >> 30)) * 0xbf58476d1ce4e5b9) & MASK64
    v = ((v ^ (v >> 27)) * 0x94d049bb133111eb) & MASK64
    return v ^ (v >> 31), s
def usz(s, mn, mx):
    v, s = next_u64(s)
    return mn + v % (mx - mn + 1), s
def i32r(s, mn, mx):
    v, s = next_u64(s)
    return mn + int(v % (mx - mn + 1)), s
G = ['a','b','c']
def ch(s, items):
    v, s = next_u64(s)
    return items[v % len(items)], s
state = 0x538454135C199494
for i in range(12):
    x, state = ch(state, G)
    x, state = i32r(state, 1, 50)
    x, state = i32r(state, 1, 10)
next_id = 13
bad_ids = []
lines = ["After 12 initial rows"]
for cycle in range(1, 9):
    action, state = usz(state, 0, 3)
    if action == 0:
        id_val = next_id
        next_id += 1
        x, state = ch(state, G)
        x, state = i32r(state, 1, 50)
        bad_ids.append(id_val)
        step = 'add-bad(id=%d)' % id_val
    elif action == 1 and bad_ids:
        idx, state = usz(state, 0, len(bad_ids) - 1)
        bad_id = bad_ids.pop(idx)
        step = 'fix-bad(id=%d,UPDATE)' % bad_id
    else:
        id_val = next_id
        next_id += 1
        x, state = ch(state, G)
        x, state = i32r(state, 1, 50)
        x, state = i32r(state, 1, 10)
        step = 'add-good(id=%d)' % id_val
    lines.append("C%d a=%d %s bad=%s" % (cycle, action, step, bad_ids))
sys.stdout.write('\n'.join(lines) + '\n')
sys.stdout.flush()
