#!/usr/bin/env python3
# one-tube.py -- Create blender model from one station's data points
from math import floor, pi, sin, cos
import os

# Station to build: uncomment just one
#tempsFile = 'CA_6719tmax.txt'
#tempsFile = 'FL_3186tmax.txt'
#tempsFile = 'MA_6486tmax.txt'
tempsFile = 'MI_0230tmax.txt'
#tempsFile = 'TX_6794tmax.txt'
#tempsFile = 'WA_0008tmax.txt'

# Settings and constants
dataDir = os.path.join(os.environ['HOME'], 'Dropbox/climate-tubes/data')
dataFile = os.path.join(dataDir, tempsFile)
dataSuffix = 'tmax.txt'
initialAngle = -pi/2   #  0=West, π/2=South, -π/2=North, π=East
thickness = 0.8
radius = 0.6
daysPerYear = 365
dailyHeight = 0.1 / daysPerYear
maxByte = 0xFF
interpolateThreshold = 14    # Interpolate up to this many missing days
normalizeToDailyRange = True # Else it normalizes across all temps
missingTemp = -999
constantColorValue = 1.0
#palette = [0x2c7bb6, 0xabd9e9, 0xffffbf, 0xfdae61, 0xd7191c]
# http://colorbrewer2.org/?type=diverging&scheme=RdYlBu&n=9
palette = [0x4575b4,
           0x74add1,
           0xabd9e9,
           0xe0f3f8,
           0xffffbf,
           0xfee090,
           0xfdae61,
           0xf46d43,
           0xd73027]

def dataFiles():
    """Generate suitable file paths from our dataDir.

    >>> fs = list(dataFiles())
    >>> len(fs) > 0
    True
    >>> fs[0].startswith(dataDir)
    True
    """
    for f in os.listdir(dataDir):
        if f.endswith(dataSuffix):
            yield os.path.join(dataDir, f)

def parseTemp(line):
    """Extract the temperature from one line of data.

    >>> parseTemp("046719   1 1911  1  1   65")
    65
    >>> parseTemp("046719   9 1911  1  9 -999")
    -999
    """
    return int(line.split()[-1])

def isAbsent(t):
    """Report whether a temperature represents missing data.

    >>> isAbsent(missingTemp)
    True
    >>> isAbsent(42)
    False
    """
    return t == missingTemp

def isPresent(t):
    """Report whether a temperature represents valid data.

    >>> isPresent(missingTemp)
    False
    >>> isPresent(42)
    True
    """
    return not isAbsent(t)

def allTempsFrom(filePath):
    """Generate all temperatures from given file, including -999s.

    >>> ts = list(allTempsFrom(dataFile))
    >>> len(ts)
    36500
    >>> missingTemp in ts
    True
    """
    with open(filePath) as instream:
        for line in instream:
            yield parseTemp(line)

def dailyAverages(temps):
    sums = [0] * daysPerYear
    counts = [0] * daysPerYear
    for i in range(len(temps)):
        if isPresent(temps[i]):
            d = i % daysPerYear
            sums[d] += temps[i]
            counts[d] += 1
    # MAYBE guard against division by zero
    return [(sums[i]/counts[i]) for i in range(daysPerYear)]

def dailyRanges(temps):
    mins = [999] * daysPerYear
    maxes = [-999] * daysPerYear
    for i in range(len(temps)):
        if isPresent(temps[i]):
            d = i % daysPerYear
            if temps[i] > maxes[d]: maxes[d] = temps[i]
            if temps[i] < mins[d]: mins[d] = temps[i]
    return (mins, maxes)

def stripMissing(temps):
    """Filter that preserves valid temperatures only.

    >>> list(stripMissing([missingTemp, 24, missingTemp, 34]))
    [24, 34]
    >>> list(stripMissing([missingTemp, missingTemp]))
    []
    >>> list(stripMissing([90, 81]))
    [90, 81]
    """
    for t in temps:
        if isPresent(t):
            yield t

def findExtrema(files):
    """Return highest and lowest values across all data files.

    >>> lo,hi = findExtrema(dataFiles())
    >>> hi > lo
    True
    >>> lo != missingTemp
    True
    >>> hi != missingTemp
    True
    """
    highest = missingTemp
    lowest = 999
    for f in files:
        for t in stripMissing(allTempsFrom(f)):
            if t > highest: highest = t
            if t < lowest: lowest = t
    return lowest, highest

def replaceMissing(temps):
    """Replace missing temps with average of their neighbors, up to
interpolation threshold. Report number of replacements and the longest
missing sequence. NOTE: Assumes that no data file begins or ends with -999!

    >>> replaceMissing([4, 1, 9])
    (0, 0, [4, 1, 9])

    >>> replaceMissing([4, missingTemp, 7])
    (1, 1, [4, 5.5, 7])

    >>> replaceMissing([4, missingTemp, missingTemp, missingTemp, 7, missingTemp, 9])
    (4, 3, [4, 5.5, 5.5, 5.5, 7, 8.0, 9])
    """
    prev = None
    numReplaces = 0
    longestMissingSeq = 0
    for i in range(len(temps)):
        if temps[i] == missingTemp:        # search forward
            j = i+1
            while temps[j] == missingTemp:
                j += 1
            avg = (prev + temps[j]) / 2
            missingSize = j - i
            if missingSize > longestMissingSeq:
                longestMissingSeq = missingSize
            if missingSize <= interpolateThreshold:
                # Fill i up to j
                for k in range(i,j):
                    temps[k] = avg
                    numReplaces += 1
        else:
            prev = temps[i]
    return numReplaces, longestMissingSeq, temps

def normalizeTemps(low, high, temps):
    """Generate a list scaled to the range {0..1}.

    >>> list(normalizeTemps(3, 5, [3, 4, 5, 4]))
    [0.0, 0.5, 1.0, 0.5]

    >>> list(normalizeTemps(3, 7, [3, 4, 5, missingTemp, 6]))
    [0.0, 0.25, 0.5, -999, 0.75]
    """
    span = high - low
    for t in temps:
        yield (t - low) / span if isPresent(t) else t

def normalizeDailyTemps(mins, maxes, temps):
    for i in range(len(temps)):
        d = i % daysPerYear
        t = temps[i]
        span = maxes[d] - mins[d]
        yield (t - mins[d]) / span if isPresent(t) else t

def vertexForTemp(t, i):
    """Report the x,y,z coordinate for given temperature and day."""
    year = floor(i/daysPerYear)
    day = i % daysPerYear
    theta = initialAngle - (day * 2 * pi / daysPerYear)
    r = t * thickness + radius
    x = r * cos(theta)
    y = r * sin(theta)
    z = i * dailyHeight
    return (x, y, z)

def equalIntervals(values):
    """Answer a list that pairs each value with a threshold.

    >>> list(equalIntervals([3,8,10,24]))
    [(0.25, 3), (0.5, 8), (0.75, 10), (1.0, 24)]
    """
    threshold = delta = 1.0 / len(values)
    for v in values:
        yield (threshold, v)
        threshold += delta

def colorForNormalizedTemp(colors, t):
    """Report the color, as a single integer, for given temperature.
Uses the `palette` setting defined above.

    >>> cs = list(equalIntervals([0x123456, 0x789abc, 0xdef012]))
    >>> '%06x' % colorForNormalizedTemp(cs, 0.25)
    '123456'
    """
    for threshold, color in colors:
        if t <= threshold:
            return color

def expandRGB(color):
    """Expand a single-integer color into a list in range 0..1.

    >>> expandRGB(0xff428a)
    [1.0, 0.25882352941176473, 0.5411764705882353]
    """
    r = (color >> 16) & maxByte
    g = (color >>  8) & maxByte
    b = (color >>  0) & maxByte
    return [r/maxByte, g/maxByte, b/maxByte]

def showPaletteHSV():
    import colorsys
    for c in palette:
        r, g, b = expandRGB(c)
        print('#%06x' % c, colorsys.rgb_to_hsv(r, g, b))

def showPaletteHTML(palette):
    for c in palette:
        print("""<div style="float:left; border:1px solid black;
margin: 2px; width:80px; height:80px; background:#%06x"></div>"""
              % c)

def normalizeColorValues(palette, value):
    import colorsys
    for c in palette:
        r, g, b = expandRGB(c)
        h, s, v = colorsys.rgb_to_hsv(r, g, b)
        r, g, b = colorsys.hsv_to_rgb(h, s, value)
        yield ((int(r*maxByte) << 16) +
               (int(g*maxByte) <<  8) +
               (int(b*maxByte) <<  0))

def triangularize(temps):
    """Return a list of faces, where each faces is 3 vertex indices."""
    faces = []
    for today in range(daysPerYear+1, len(temps)):
        yesterday = today - 1
        yesterYear = today - daysPerYear - 1
        lastYear = today - daysPerYear
        if isPresent(temps[today]) and isPresent(temps[yesterday]) and isPresent(temps[yesterYear]):
            faces.append((yesterYear, yesterday, today))
        if isPresent(temps[today]) and isPresent(temps[yesterYear]) and isPresent(temps[lastYear]):
            faces.append((lastYear, yesterYear, today))
    return faces

def saveToBlender(vertexPoints, vertexColors, faces):
    import bpy
    name, _ = os.path.splitext(os.path.basename(tempsFile))
    me = bpy.data.meshes.new(name+'Mesh')    # create a new mesh
    ob = bpy.data.objects.new(name, me)      # create an object with that mesh
    ob.location = bpy.context.scene.cursor_location # position object at 3d-cursor
    bpy.context.scene.objects.link(ob)              # Link object to scene

    # Fill the mesh with verts, edges, faces
    me.from_pydata(vertexPoints,[],faces)
    me.update(calc_edges=True)    # Update mesh with new data

    # Attach the vertex colors
    color_map_collection = me.vertex_colors
    if len(color_map_collection) == 0:
        color_map_collection.new()
    color_map = color_map_collection['Col']
    for i in range(len(me.polygons)):
        poly = me.polygons[i]
        vs = faces[i]
        for j in range(3):
            color_map.data[i*3+j].color = vertexColors[vs[j]]

def comparePalettes():
    showPaletteHTML(palette)
    print("<p>Variable</p>")
    v = 1.0
    while v >= 0.9:
        print("""<div style="clear:both"></div>""")
        showPaletteHTML(list(normalizeColorValues(palette, v)))
        print("<p>%f</p>" % v)
        v -= 0.02

def main():
    temps = list(allTempsFrom(dataFile))
    print("Got %d samples from %s" % (len(temps), tempsFile))

    if normalizeToDailyRange:
        mins, maxes = dailyRanges(temps)
    else:
        lowest, highest = findExtrema(dataFiles())
        print("Range across all files is %d to %d" % (lowest, highest))
        num, longest, temps = replaceMissing(temps)
        print("Replaced %d missing samples; longest consecutive seq was %d days" %
              (num, longest))

    if normalizeToDailyRange:
        temps = list(normalizeDailyTemps(mins, maxes, temps))
    else:
        temps = list(normalizeTemps(lowest, highest, temps))

    normalPalette = list(normalizeColorValues(palette, constantColorValue))
    colors = list(equalIntervals(normalPalette))

    vertexColors = [expandRGB(colorForNormalizedTemp(colors, t)) for t in temps]
    vertexPoints = []
    for i in range(len(temps)):
        vertexPoints.append(vertexForTemp(temps[i], i))
    print("Calculated %d vertices" % len(vertexPoints))

    faces = triangularize(temps)
    print("Specified %d triangles" % len(faces))

    try:
        import bpy
    except:
        print("Not running in blender, so we'll just dump the data.")
        print(colors)
        for v in vertexPoints:
            print(v)
        for c in vertexColors:
            print(c)
        for f in faces:
            print(f)
        exit(0)

    saveToBlender(vertexPoints, vertexColors, faces)

if __name__ == "__main__":
    #showPaletteHSV()
    main()
    #comparePalettes()
