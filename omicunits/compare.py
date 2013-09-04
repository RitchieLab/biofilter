#!/usr/bin/env python

import apsw
import collections
import itertools
import math
import random
import sys
import time

db = apsw.Connection(sys.argv[1])
cursor = db.cursor()
report = open(sys.argv[2],'wb') if (len(sys.argv) > 2) else None

# load namespace definitions
namespaceName = dict()
namespaceID = dict()
for row in cursor.execute("select namespace_id,namespace from namespace;"):
	namespaceName[row[0]] = row[1]
	namespaceID[row[1]] = row[0]

# load namespace aliases
alias = dict()
if 0:
	for row in cursor.execute("select namespace_id1,name1,namespace_id2,name2 from unit_name_name where namespace_id1 = namespace_id2;"):
		name1 = (row[0],row[1])
		name1a = alias.get(name1,name1)
		name2 = (row[2],row[3])
		name2a = alias.get(name2,name2)
		name = min(name1,name1a,name2,name2a)
		alias[name1] = name
		alias[name1a] = name
		alias[name2] = name
		alias[name2a] = name
		del alias[name]
	print "%d namespace aliases" % (len(alias),)

# load name graph
graph = collections.defaultdict(set)
nameIndex = dict()
nameNamespaceID = list()
nameName = list()
numEdges = 0
for row in cursor.execute("select namespace_id1,name1,namespace_id2,name2 from unit_name_name;"):
	name1 = (row[0],row[1])
	name2 = (row[2],row[3])
	while name1 in alias:
		name1 = alias[name1]
	while name2 in alias:
		name2 = alias[name2]
	if (name1[0] == namespaceID['ensembl_gid'] and not name1[1].startswith('ENSG')):
		continue
	if (name2[0] == namespaceID['ensembl_gid'] and not name2[1].startswith('ENSG')):
		continue
	if name1 != name2:
		numEdges += 1
		if name1 not in nameIndex:
			nameIndex[name1] = len(nameNamespaceID)
			nameNamespaceID.append(name1[0])
			nameName.append(name1[1])
		if name2 not in nameIndex:
			nameIndex[name2] = len(nameNamespaceID)
			nameNamespaceID.append(name2[0])
			nameName.append(name2[1])
		graph[nameIndex[name1]].add(nameIndex[name2])
		graph[nameIndex[name2]].add(nameIndex[name1])
print "raw graph: %d nodes, %d edges" % (len(graph),numEdges)

# load regions
regionIndex = dict()
regionSpan = list()
regionNames = collections.defaultdict(set)
nameChrRegions = collections.defaultdict(lambda: collections.defaultdict(set))
for row in cursor.execute("select rn.namespace_id, rn.name, r.chr, r.posMin, r.posMax from region_name as rn join region as r using (region_id)"):
	name = (row[0],row[1])
	while name in alias:
		name = alias[name]
	if name in nameIndex:
		n = nameIndex[name]
		region = (row[2],row[3],row[4])
		if region not in regionIndex:
			regionIndex[region] = len(regionSpan)
			regionSpan.append(region)
		r = regionIndex[region]
		regionNames[r].add(n)
		nameChrRegions[n][row[2]].add(r)
print "%d names have %d regions" % (len(nameChrRegions),len(regionNames))
alias = nameIndex = regionIndex = None


def connectedComponents(nodes=None, namespaces=None):
	_graph = graph
	_nameNamespaceID = nameNamespaceID
	
	nodes = set(n for n in (nodes or _graph) if ((not namespaces) or (namespaceName[nameNamespaceID[n]] in namespaces)))
 	stack = list()
	while nodes:
		node = nodes.pop()
		component = {node}
		stack.append(node)
		while stack:
			node = stack.pop()
			for neighbor in _graph[node]:
				if neighbor in nodes:
					component.add(neighbor)
					nodes.remove(neighbor)
					stack.append(neighbor)
		yield component
#connectedComponents()


def nameComponents(components):
	names = collections.defaultdict(set)
	for i,c in enumerate(components):
		for n in c:
			names[n].add(i)
	return names
#nameComponents()


def defineUnits(cores, maxdist=0):
	# cores = [ {nsid1}, {nsid2,nsid3}, ... ]
	_graph = graph
	_nameNamespaceID = nameNamespaceID
	
	# each "core" is a set of nsIDs that may define new units; however,
	# nsIDs in later cores will be merged into units defined by earlier cores
	# if they're directly connected to an earlier unit's core
	nsidCore = dict()
	for c,core in enumerate(cores):
		for nsid in core:
			if nsid not in nsidCore:
				nsidCore[nsid] = c+1
	
	# identify all names that will be cores of new units
	nameDist = dict()
	nameUnits = collections.defaultdict(set)
	unitNames = list()
	stack = list()
	for core in cores:
		for n0 in _graph:
			if (_nameNamespaceID[n0] not in core) or (n0 in nameDist):
				continue
			u = len(unitNames)
			nameDist[n0] = 0
			nameUnits[n0].add(u)
			names = {n0}
			stack.append(n0)
			while stack:
				n1 = stack.pop()
				c1 = nsidCore[_nameNamespaceID[n1]]
				for n2 in _graph[n1]:
					c2 = nsidCore.get(_nameNamespaceID[n2],0)
					if (c2 >= c1) and (n2 not in names):
						nameDist[n2] = 0
						nameUnits[n2].add(u)
						names.add(n2)
						stack.append(n2)
			unitNames.append(names)
	#print "(found %d unit cores with %d names)" % (len(unitNames),len(nameUnits))
	
	# identify all names that will be cores of new units
	"""
	nameDist = dict()
	nameUnits = collections.defaultdict(set)
	unitNames = list()
	stack = list()
	for n0 in _graph:
		o0 = nsidOrder.get(_nameNamespaceID[n0])
		if o0 and (n0 not in nameDist):
			names = {n0}
			stack.append(n0)
			while stack:
				n1 = stack.pop()
				o1 = nsidOrder.get(_nameNamespaceID[n1])
				for n2 in _graph[n1]:
					o2 = nsidOrder.get(_nameNamespaceID[n2])
					if (not o2) or (n2 in names):
						pass
					elif o2 >= o1:
						names.add(n2)
						stack.append(n2)
					elif (o2 < o0) and (o1 == o0):
						names = None
						stack = list()
						break
			if names:
				for n in names:
					nameDist[n] = 0
					nameUnits[n].add(len(unitNames))
				unitNames.append(names)
	"""
	
	# assign additional names to each unit using BFS
	nsidStop = set(id for ns,id in namespaceID.iteritems() if ns in {'label','description','utype'}) #'ensembl_pid','refseq_pid','uniprot_pid'
	queue = collections.deque(nameDist.iterkeys())
	while queue:
		n1 = queue.popleft()
		d2 = nameDist[n1] + 1
		if maxdist and (d2 > maxdist):
			continue
		units = nameUnits[n1]
		for n2 in _graph[n1]:
			if n2 not in nameDist:
				nameDist[n2] = d2
				if _nameNamespaceID[n2] not in nsidStop:
					queue.append(n2)
			if nameDist[n2] == d2:
				nameUnits[n2] |= units
				for u in units:
					unitNames[u].add(n2)
	
	return unitNames,nameUnits
#defineUnits()


def componentBreakdown(components, namespace, head=None, tail=None):
	_nameNamespaceID = nameNamespaceID
	
	nsid = namespaceID[namespace]
	breakdown = collections.defaultdict(int)
	for c in components:
		breakdown[sum((1 if (_nameNamespaceID[n] == nsid) else 0) for n in c)] += 1
	l = [("%dx%d" % (breakdown[n],n)) for n in sorted(breakdown)]
	if (head or tail) and (((head or 0) + (tail or 0)) < len(l)):
		l = l[:(head or 0)] + ["..."] + l[-(tail or 0):]
	return l
#componentBreakdown


def reverseBreakdown(names, namespace, head=None, tail=None):
	_graph = graph
	_nameNamespaceID = nameNamespaceID
	
	nsid = namespaceID[namespace]
	breakdown = collections.defaultdict(int)
	empty = list()
	for n in _graph:
		if _nameNamespaceID[n] == nsid:
			breakdown[len(names.get(n,empty))] += 1
	l = [("%dx%d" % (breakdown[n],n)) for n in sorted(breakdown)]
	if (head or tail) and (((head or 0) + (tail or 0)) < len(l)):
		l = l[:(head or 0)] + ["..."] + l[-(tail or 0):]
	return l
#reverseBreakdown


def nameBreakdown(nameUnits, namespace):
	_nameNamespaceID = nameNamespaceID
	
	b0 = b1 = bD = bN = 0
	nsid = namespaceID[namespace]
	hits = dict()
	for n in graph.iterkeys():
		if _nameNamespaceID[n] == nsid:
			units = nameUnits.get(n)
			if (not units) or (len(units) < 1):
				b0 += 1
			elif len(units) > 1:
				bN += 1
			else:
				for u in units:
					if u not in hits:
						b1 += 1
						hits[u] = 1
					elif hits[u] == 1:
						b1 -= 1
						bD += 2
						hits[u] = 2
					else:
						bD += 1
	return (bN,bD,b1,b0)
#nameBreakdown()


def matchBreakdown(nameUnits, namespaces=None):
	_nameNamespaceID = nameNamespaceID
	
	namespaceIDs = set(namespaceID[ns] for ns in (namespaces or namespaceID))
	bX = b0 = b1 = bN = 0
	for n1,n2s in graph.iteritems():
		if _nameNamespaceID[n1] not in namespaceIDs:
			continue
		for n2 in n2s:
			if _nameNamespaceID[n2] not in namespaceIDs:
				continue
			if n2 <= n1:
				continue
			u1 = nameUnits.get(n1)
			u2 = nameUnits.get(n2)
			if (not u1) or (not u2):
				b0 += 1
			elif len(u1 & u2) < 1:
				bX += 1
			elif len(u1 | u2) > 1:
				bN += 1
			else:
				b1 += 1
	return (bN,b1,bX,b0)
#matchBreakdown()				


def renderPlotCell(counts, pairs=False):
	total = sum(counts)/100.0
	percents = list()
	heights = list()
	extra = 0
	for i in xrange(len(counts)):
		p = int(round(counts[i]/total))
		percents.append(p)
		if counts[i] and not p:
			heights.append(1)
			extra += 1
		else:
			heights.append(p)
	while extra:
		heights[heights.index(max(heights))] -= 1
		extra -= 1
	out = list()
	for i in xrange(len(counts)):
		out.append(heights[i])
		out.append(counts[i])
		out.append(percents[i])
	if pairs:
		return """
			<td>
				<div class="bars">
					<div class="mult" style="height:%d%%"><span>%d (%d%%)</span></div>
					<div class="uniq" style="height:%d%%"><span>%d (%d%%)</span></div>
					<div class="miss" style="height:%d%%"><span>%d (%d%%)</span></div>
					<div class="none" style="height:%d%%"><span>%d (%d%%)</span></div>
				</div>
			</td>""" % tuple(out)
	else:
		return """
			<td>
				<div class="bars">
					<div class="mult" style="height:%d%%"><span>%d (%d%%)</span></div>
					<div class="dupe" style="height:%d%%"><span>%d (%d%%)</span></div>
					<div class="uniq" style="height:%d%%"><span>%d (%d%%)</span></div>
					<div class="none" style="height:%d%%"><span>%d (%d%%)</span></div>
				</div>
			</td>""" % tuple(out)
#renderPlotCell()


def reportResults(label, unitNames, nameUnits, report=None):
	print label,"units:",len(unitNames)
	if report:
		report.write("""
		<tr>
			<th>%s</th>
			<td>%d</td>""" % (label,len(unitNames)))
	counts = matchBreakdown(nameUnits,set(ns for ns,id in namespaceID.iteritems() if ns not in {'label','description','utype'}))
	print " all pairs: %dx~, %dx+, %dx-, %dx0" % counts
	if report:
		report.write(renderPlotCell(counts,True))
	counts = matchBreakdown(nameUnits,{'hgnc_gid','entrez_gid','ensembl_gid'})
	print " HNE pairs: %dx~, %dx+, %dx-, %dx0" % counts
	if report:
		report.write(renderPlotCell(counts,True))
	counts = matchBreakdown(nameUnits,{'entrez_gid','ensembl_gid'})
	print "  NE pairs: %dx~, %dx+, %dx-, %dx0" % counts
	if report:
		report.write(renderPlotCell(counts,True))
	print " > labl:",", ".join(componentBreakdown(unitNames,'label',4,4))
	counts = nameBreakdown(nameUnits,'ensembl_gid')
	print " < ensg: %dx2+, %dxD, %dx1, %dx0" % counts
	if report:
		report.write(renderPlotCell(counts))
	counts = nameBreakdown(nameUnits,'hgnc_gid')
	print " < hgnc: %dx2+, %dxD, %dx1, %dx0" % counts
	if report:
		report.write(renderPlotCell(counts))
	counts = nameBreakdown(nameUnits,'entrez_gid')
	print " < ncbi: %dx2+, %dxD, %dx1, %dx0" % counts
	if report:
		report.write(renderPlotCell(counts))
#	if report:
#		cT = tuple(cE[i]+cH[i]+cN[i] for i in xrange(len(cE)))
#		report.write(renderPlotCell(cT))
#		report.write(renderPlotCell(cE))
#		report.write(renderPlotCell(cH))
#		report.write(renderPlotCell(cN))
	if report:
		report.write("""
		</tr>""")
#reportResults()

def closestPoint(a, b, p):
	ab = (b[0]-a[0], b[1]-a[1])
	ap = (p[0]-a[0], p[1]-a[1])
	abDap = ab[0]*ap[0] + ab[1]*ap[1]
	t = abDap / (ab[0]**2 + ab[1]**2)
	if t <= 0:
		return a
	if t >= 1:
		return b
	return (a[0]+t*ab[0], a[1]+t*ab[1])
#closestPoint()


def plot(unitNames, nameUnits, units, caption):
	nsIDs = {namespaceID['entrez_gid'],namespaceID['ensembl_gid']}
	names = set()
	for u in units:
		names.update(n for n in unitNames[u] if nameNamespaceID[n] in nsIDs)
	x = dict()
	xlist = collections.defaultdict(list)
	y = dict()
	ylist = collections.defaultdict(list)
	dx = dict()
	dy = dict()
	nn = list()
	en = list()
	# set initial node positions and pre-calculate attractive forces
	scale = len(names)
	charge = 2500.0
	friction = 0.75
	init = (16 * len(names)) / math.pi
	for n1 in names:#sorted(names, key=lambda n: (nameNamespaceID[n],nameName[n])):
		a = (2 * math.pi * len(x) / len(names))
		x[n1] = int(init * math.sin(a))
		y[n1] = int(init * math.cos(a))
		for n2 in names:
			if n2 > n1:
				f = 0.0
				u = len(nameUnits[n1] & nameUnits[n2] & units)
				f += 0.01 * ((u ** 0.5) if u else 0) / scale
				f += 0.01 * (1 if (n2 in graph[n1]) else -0.01)
				nn.append( (n1,n2,f) )
				if n2 in graph[n1]:
					n3s = (set().union( *(unitNames[u] for u in ((nameUnits[n1] | nameUnits[n2]) & units)) ) & units) - {n1,n2}
					en.append( (n1,n2,n3s) )
	# run force-directed layout simulation
	print ("simulating %s ..." % caption),
	phase = 3
	t0 = time.time()
	while (phase > 0) and (time.time() < t0 + 10):
		phase -= 1
		for n in names:
			a = random.random() * 2 * math.pi
			dx[n] = 10 * (2**phase) * math.cos(a)
			dy[n] = 10 * (2**phase) * math.sin(a)
			xlist[n].append(x[n])
			ylist[n].append(y[n])
			x[n] += dx[n]
			y[n] += dy[n]
		stable = 0
		while (stable < 10) and (time.time() < t0 + 15):
			for n1,n2,f in nn:
				x12 = x[n2] - x[n1]
				y12 = y[n2] - y[n1]
				dd = x12**2 + y12**2
				d = dd**0.5
				if f < 0:
					f = (f / max(1,d-30)) - (charge / max(225,dd))
				else:
					f = (f * max(1,d-30)) - (charge / max(225,dd))
				x12 = f * (x12 / max(1e-100,d))
				y12 = f * (y12 / max(1e-100,d))
				dx[n1] += x12
				dy[n1] += y12
				dx[n2] -= x12
				dy[n2] -= y12
			movement = 0
			for n in names:
				xlist[n].append(x[n])
				ylist[n].append(y[n])
				x[n] = min(max(x[n] + dx[n], -10000), 10000)
				y[n] = min(max(y[n] + dy[n], -10000), 10000)
				movement = max(movement, dx[n]**2 + dy[n]**2)
				dx[n] *= friction
				dy[n] *= friction
			if movement < 0.01:
				stable += 1
			else:
				stable = 0
		stable = 0
		while (stable < 10) and (time.time() < t0 + 15):
			for n1,n2,f in nn:
				x12 = x[n2] - x[n1]
				y12 = y[n2] - y[n1]
				dd = x12**2 + y12**2
				d = dd**0.5
				if f < 0:
					f = (f / max(1,d-30)) - (charge / max(225,dd))
				else:
					f = (f * max(1,d-30)) - (charge / max(225,dd))
				x12 = f * (x12 / max(1e-100,d))
				y12 = f * (y12 / max(1e-100,d))
				dx[n1] += x12
				dy[n1] += y12
				dx[n2] -= x12
				dy[n2] -= y12
			for n1,n2,n3s in en:
				for n3 in n3s:
					ex,ey = closestPoint( (x[n1],y[n1]), (x[n2],y[n2]), (x[n3],y[n3]) )
					xe3 = x[n3] - ex
					ye3 = y[n3] - ey
					dd = xe3**2 + ye3**2
					d = dd**0.5
					f = -500.0 / max(50,dd)
					xe3 = f * (xe3 / max(1e-100,d))
					ye3 = f * (ye3 / max(1e-100,d))
					dx[n1] += xe3 / 2
					dy[n1] += ye3 / 2
					dx[n2] += xe3 / 2
					dy[n2] += ye3 / 2
					dx[n3] -= xe3
					dy[n3] -= ye3
			movement = 0
			for n in names:
				xlist[n].append(x[n])
				ylist[n].append(y[n])
				x[n] = min(max(x[n] + dx[n], -10000), 10000)
				y[n] = min(max(y[n] + dy[n], -10000), 10000)
				movement = max(movement, dx[n]**2 + dy[n]**2)
				dx[n] *= friction
				dy[n] *= friction
			if movement < 0.01:
				stable += 1
			else:
				stable = 0
	# recenter all coordinates to the positive quadrant, with 25px buffer
	i = min(len(xlist[n]) for n in names)
	x0 = 125 - min(x.itervalues())
	y0 = 25 - min(y.itervalues())
	for n in names:
		x[n] += x0
		y[n] += y0
		for l in xrange(i):
			xlist[n][l] = "%d" % (xlist[n][l] + x0)
			ylist[n][l] = "%d" % (ylist[n][l] + y0)
	x1 = 25 + max(x.itervalues())
	y1 = 25 + max(y.itervalues())
	y1 = max(y1, x1*7/10)
	t1 = time.time()
	print " OK: %d iterations in %.2fs" % (i,t1-t0)
	with open('%s.svg' % caption,'wb') as f:
		f.write("""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
<svg version="1.1" width="%1.2fin" height="%din" viewBox="0 0 %d %d" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">"""
			% (7*x1/y1,7,int(x1),int(y1))
		)
		f.write("""
	<title>%s</title>
	<g stroke="black" stroke-width="2px">"""
			% (caption,)
		)
		for n1 in names:
			for n2 in graph[n1]:
				if (n2 > n1) and (n2 in names):
					f.write("""
		<line x1="%d" y1="%d" x2="%d" y2="%d">
			<animate begin="accessKey( )" attributeName="x1" values="%s" dur="%ds" />
			<animate begin="accessKey( )" attributeName="y1" values="%s" dur="%ds" />
			<animate begin="accessKey( )" attributeName="x2" values="%s" dur="%ds" />
			<animate begin="accessKey( )" attributeName="y2" values="%s" dur="%ds" />
		</line>"""
						% (
							x[n1], y[n1], x[n2], y[n2],
							";".join(xlist[n1]), (i/30)+1,
							";".join(ylist[n1]), (i/30)+1,
							";".join(xlist[n2]), (i/30)+1,
							";".join(ylist[n2]), (i/30)+1,
						)
					)
		f.write("""
	</g>
	<g>"""
		)
		for n in names:
			text = nameName[n]
			url = ""
			color = "black"
			if nameNamespaceID[n] == namespaceID['entrez_gid']:
				url = "http://www.ncbi.nlm.nih.gov/gene/%d" % (int(nameName[n]),)
				color = "#8080ff"
			elif nameNamespaceID[n] == namespaceID['ensembl_gid']:
				url = "http://www.ensembl.org/Homo_sapiens/Gene/Summary?g=%s" % (nameName[n],)
				if nameName[n][3] == "G":
					color = "#ff4040"
				elif nameName[n][3] == "T":
					color = "#ffc040"
				elif nameName[n][3] == "E":
					color = "#ff8040"
			f.write("""
		<a xlink:href="%s" id="n%d">
			<circle cx="%d" cy="%d" r="15px" fill="%s">
				<animate begin="accessKey( )" attributeName="cx" values="%s" dur="%ds" />
				<animate begin="accessKey( )" attributeName="cy" values="%s" dur="%ds" />
			</circle>
			<title>%s</title>
		</a>"""
				% (
					url, n, x[n], y[n], color,
					";".join(xlist[n]), (i/30)+1,
					";".join(ylist[n]), (i/30)+1,
					text,
				)
			)
		f.write("""
	</g>
	<g font-family="Verdana" font-size="8px" fill="black" text-anchor="middle">
		<set attributeName="visibility" to="hidden" begin="accessKey( )" dur="%ds" />"""
			% (
				(i/30)+1,
			)
		)
		for n in names:
			f.write("""
		<text x="%d" y="%d">%s</text>"""
				% (
					x[n], y[n]+2, (sum(len(r) for r in nameChrRegions[n].itervalues()) or ""),
				)
			)
		chrNames = collections.defaultdict(set)
		for n in names:
			for c,rs in nameChrRegions[n].iteritems():
				if rs:
					chrNames[c].add(n)
		w = min(10.0, float(y1 - 10 - (10 * len(chrNames))) / sum(len(ns) for ns in chrNames.itervalues())) if chrNames else 0
		f.write("""
	</g>
	<g font-family="Verdana" font-size="10px" fill="black" text-anchor="left" stroke-width="%1.2fpx">"""
			% (w,)
		)
		y1 = 5.0
		for c in sorted(chrNames):
			rMin = min(regionSpan[r][1] for r in set().union( *(nameChrRegions[n][c] for n in chrNames[c]) ))
			rMax = max(regionSpan[r][2] for r in set().union( *(nameChrRegions[n][c] for n in chrNames[c]) ))
			y1 += 10.0
			f.write("""
		<text x="%1.1f" y="%1.1f">chr%d ~%sMB</text>"""
				% (5.0,y1-2.5,c,"-".join(str(s) for s in sorted({int(rMin/1e6),int(rMax/1e6)})))
			)
			for n in sorted(chrNames[c], key=y.get):
				color1 = "#000000"
				color2 = "#404040"
				if nameNamespaceID[n] == namespaceID['entrez_gid']:
					color1 = "#000080"
					color2 = "#8080ff"
				elif nameNamespaceID[n] == namespaceID['ensembl_gid']:
					color1 = "#800000"
					color2 = "#ff8080"
				f.write("""
		<g stroke="%s">
			<set attributeName="stroke" to="%s" begin="n%d.mouseover" end="n%d.mouseout"/>"""
					% (color1, color2, n, n)
				)
				y1 += w
				for r in nameChrRegions[n][c]:
					x0 = 5+100.0*(regionSpan[r][1]-rMin)/(rMax-rMin)
					x1 = max(5+100.0*(regionSpan[r][2]-rMin)/(rMax-rMin), x0+0.1)
					f.write("""
			<line x1="%1.1f" y1="%1.1f" x2="%1.1f" y2="%1.1f" />"""
						% (x0, y1-w/2, x1, y1-w/2)
					)
				#foreach region
				f.write("""
		</g>"""
				)
			#foreach name
		#foreach chr
		f.write("""
	</g>
</svg>"""
		)
	#with f
#	for u in units:
#		names = set("%s:%s" % (namespaceName[nameNamespaceID[n]],nameName[n]) for n in unitNames[u])
#		print " %s%s\t%s" % (("N" if u in unitsA else " "),("E" if u in unitsB else " "),",".join(names))
#plot()


# report header
if report:
	allNamespaces = set(ns for ns,id in namespaceID.iteritems() if ns not in {'label','description','utype'})
	ehnNamespaces = {'hgnc_gid','entrez_gid','ensembl_gid'}
	enNamespaces = {'entrez_gid','ensembl_gid'}
	namespaceTally = collections.Counter()
	allPairTally = ehnPairTally = enPairTally = 0
	for n1,n2s in graph.iteritems():
		ns1 = namespaceName[nameNamespaceID[n1]]
		namespaceTally[ns1] += 1
		for n2 in n2s:
			ns2 = namespaceName[nameNamespaceID[n2]]
			if n2 <= n1:
				continue
			if (ns1 in allNamespaces) and (ns2 in allNamespaces):
				allPairTally += 1
			if (ns1 in ehnNamespaces) and (ns2 in ehnNamespaces):
				ehnPairTally += 1
			if (ns1 in enNamespaces) and (ns2 in enNamespaces):
				enPairTally += 1
	report.write("""
<html>
<head>
	<style type="text/css">
DIV.notes {
	float: right;
}
TABLE {
	float: left;
	font-size: 11pt;
}
BODY:target > TABLE#table0 {
	display: none;
}
BODY:not(:target) > TABLE#table1 {
	display: none;
}
THEAD TH {
	background: #aaa;
	padding: 0.1em 0.2em;
}
THEAD TD {
	min-width: 80px;
	background: #ddd;
	text-align: center;
	padding: 0.1em 0.2em;
}
THEAD TR:first-child TD {
	font-weight: bold;
}
TBODY TH {
	background: #ddd;
	text-align: left;
	padding: 0.1em 0.2em;
}
TBODY TD {
	background: #ddd;
	text-align: center;
	padding: 0.1em 0.2em;
}
DIV.bars {
	display: inline-block;
	width: 20px;
	height: 100px;
}
DIV.bars > SPAN {
	display: block;
	border: 1px solid black;
	border-radius: 0.5em;
	background: #ddd;
	padding: 0 0.25em;
	z-index: 1;
	font-size: 10pt;
}
DIV.bars > DIV.mult {
	background-color: #e70;
}
DIV.bars > DIV.dupe {
	background-color: #0dd;
}
DIV.bars > DIV.uniq {
	background-color: #080;
}
DIV.bars > DIV.miss {
	background-color: #f00;
}
DIV.bars > DIV.none {
	background-color: #448;
}

DIV.bars > DIV > SPAN {
	visibility: hidden;
	border: 1px solid black;
	border-radius: 0.5em;
	background: #ddd;
	padding: 0 0.25em;
	font-size: 10pt;
	white-space: nowrap;
}
DIV.bars:hover > DIV > SPAN {
	visibility: visible;
}
DIV.bars > DIV.mult > SPAN {
	background-color: #fda;
}
DIV.bars > DIV.dupe > SPAN {
	background-color: #cff;
}
DIV.bars > DIV.uniq > SPAN {
	background-color: #cfc;
}
DIV.bars > DIV.miss > SPAN {
	background-color: #fcc;
}
DIV.bars > DIV.none > SPAN {
	background-color: #ccf;
}
DIV.bars > DIV > SPAN {
	float: right;
	margin: -75% 110% 75% 0;
}
.key {
	text-align: left;
	vertical-align: top;
}
	</style>
</head>
<body id="alt">
""")


def analyze_units(unitNames):
	# 80030 1N1E, 1211 1N2E, 304 2N1E, 472 2N2E
	data = collections.defaultdict(list)
	nsIDs = {namespaceID['entrez_gid'],namespaceID['ensembl_gid']}
#	extras = 0
	for u,names in enumerate(unitNames):
		numN = sum(1 for c in connectedComponents(names, {'entrez_gid'}))
		numE = sum(1 for c in connectedComponents(names, {'ensembl_gid'}))
		regions = set()
		for n in names:
#			if nameNamespaceID[n] in nsIDs:
				regions.update( *nameChrRegions[n].itervalues() )
#			else:
#				extras += sum(len(cr) for cr in nameChrRegions[n].itervalues())
		regions = sorted(regionSpan[r] for r in regions)
		numC = len(set(r[0] for r in regions))
		curC = None
		numG = 0
		maxG = 0
		for c,l,r in regions:
			if curC != c:
				curC = c
				curR = r
			elif l > curR:
				numG += 1
				maxG = max(maxG, l - curR)
			curR = max(curR, r)
		numN = 2 if numN > 1 else numN
		numE = 2 if numE > 1 else numE
		numC = 2 if numC > 1 else numC
		data[ (numN,numE,numC) ].append( (maxG,u) )
#	print extras
	for key in sorted(data):
		numN,numE,numC = key
		lst = data[key]
		lst.sort()
		n = len(lst)
		print "%d N, %d E, %d C : %-5d : %d / %d / %d / %d / %d / %d / %d" % (numN,numE,numC,n,lst[0][0],lst[n/20][0],lst[n/4][0],lst[n/2][0],lst[-n/4][0],lst[-n/20][0],lst[-1][0])
	for key,lst in data.iteritems():
		numN,numE,numC = key
		maxG,u = lst[-len(lst)/4]
		plot(unitNames, nameUnits, {u}, "%dn%de%dc_%dx_%dg" % (numN,numE,numC,len(lst),maxG))
#analyze_units()


def analyze_starfish(unitNames, nameUnits):
	unitsN1E1 = set()
	unitsN1E2 = set()
	unitsN2E1 = set()
	unitsN2E2 = set()
	for u,names in enumerate(unitNames):
		numN = sum(1 for c in connectedComponents(names, {'entrez_gid'}))
		numE = sum(1 for c in connectedComponents(names, {'ensembl_gid'}))
		if (numN > 1) and (numE > 1):
			unitsN2E2.add(u)
		elif numN > 1:
			unitsN2E1.add(u)
		elif numE > 1:
			unitsN1E2.add(u)
		else:
			unitsN1E1.add(u)
	print "%d 1N1E, %d 1N2E, %d 2N1E, %d 2N2E" % (len(unitsN1E1),len(unitsN1E2),len(unitsN2E1),len(unitsN2E2))
	
	"""
1. how many have all legs contained within the core
2. how many have all legs at least overlapping the core
3. how many have no overlap, and what is the min distance to the core
4. how many have one overlap, one non-overlap, and what is the max gap outside the core region
5. how many have no regions
"""
	status = collections.Counter()
	someDist = list()
	noneDist = list()
	someGap = list()
	noneGap = list()
	for u in unitsN1E2:
		cores = set()
		legs = set()
		for n in unitNames[u]:
			if nameNamespaceID[n] == namespaceID['entrez_gid']:
				cores.update( *(nameChrRegions[n].itervalues()) )
			elif nameNamespaceID[n] == namespaceID['ensembl_gid']:
				legs.update( *(nameChrRegions[n].itervalues()) )
		if (not cores) and (not legs):
			status['no-regions'] += 1
			continue
		if not cores:
			status['no-core-regions'] += 1
			continue
		if not legs:
			status['no-leg-regions'] += 1
			continue
		
		cores = list(regionSpan[r] for r in cores)
		cores.sort()
		cC,lC,rC = cores[0]
		for c,l,r in cores:
			if (c != cC) or (l > rC) or (r < lC):
				cC = None
				break
			lC,rC = min(lC,l),max(rC,r)
		if not cC:
			status['multi-core'] += 1
			continue
		
		legs = list(regionSpan[r] for r in legs)
		legs.sort()
		allIn = allOver = allChr = True
		someOver = False
		minDist = maxGap = None
		rL = None
		for c,l,r in legs:
			if (c != cC) or (l < lC) or (r > rC):
				allIn = False
			if (c != cC) or (l > rC) or (r < lC):
				allOver = False
				if c == cC:
					gap = max(0, l - (rL or l))
					maxGap = max(maxGap or gap, gap)
			else:
				someOver = True
				if rL and (rL < lC):
					gap = max(0, l - (rL or l))
					maxGap = max(maxGap or gap, gap)
			rL = r
			if c == cC:
				dist = max(l-rC,lC-r)
				if dist > 0:
					minDist = min(minDist or dist, dist)
			else:
				allChr = False
		if allIn:
			status['all-contained'] += 1
		elif allOver:
			status['all-overlap'] += 1
		elif someOver and allChr:
			status['some-overlap-1chr'] += 1
			someDist.append(minDist)
			someGap.append(maxGap)
		elif someOver and not allChr:
			status['some-overlap-2chr'] += 1
		elif allChr:
			status['no-overlap-1chr'] += 1
			noneDist.append(minDist)
			noneGap.append(maxGap)
		else:
			status['no-overlap-2chr'] += 1
	print status
	someDist.sort()
	someGap.sort()
	nd,ng = len(someDist),len(someGap)
	print "some dist: %d [%d/%d/%d/%d/%d]" % (nd,someDist[0],someDist[nd/4],someDist[nd/2],someDist[-nd/4],someDist[-1])
	print someDist
	print "some gap:  %d [%d/%d/%d/%d/%d]" % (ng,someGap[0], someGap[ng/4], someGap[ng/2], someGap[-ng/4], someGap[-1])
	print someGap
	noneDist.sort()
	noneGap.sort()
	nd,ng = len(noneDist),len(noneGap)
	print "none dist: %d [%d/%d/%d/%d/%d]" % (nd,noneDist[0],noneDist[nd/4],noneDist[nd/2],noneDist[-nd/4],noneDist[-1])
	print "none gap:  %d [%d/%d/%d/%d/%d]" % (ng,noneGap[0], noneGap[ng/4], noneGap[ng/2], noneGap[-ng/4], noneGap[-1])
#analyze_starfish()


for maxdist in (0,1):
	if report:
		if maxdist == 0:
			link = 'inf. hops<br>(<a href="#alt">toggle</a>)'
		else:
			link = '%d hop%s<br>(<a href="#main">toggle</a>)' % (maxdist,'' if maxdist==1 else 's')
		report.write("""
<table id="table%d">
	<thead>
		<tr>
			<td>%s</td>
			<th>Identifier</th>
			<td><i>(all<br>pairs)</i></td>
			<td><i>(EHN<br>pairs)</i></td>
			<td><i>(EN<br>pairs)</i></td>
			<td>Ensembl</td>
			<td>HGNC</td>
			<td>NCBI /<br>Entrez</td>
		</tr>
		<tr>
			<th>Algorithm</th>
			<th>Count</th>
			<td>%d</td>
			<td>%d</td>
			<td>%d</td>
			<td>%d</td>
			<td>%d</td>
			<td>%d</td>
		</tr>
	</thead>
	<tbody>""" % (maxdist,link,allPairTally,ehnPairTally,enPairTally,namespaceTally['ensembl_gid'],namespaceTally['hgnc_gid'],namespaceTally['entrez_gid']))
	#if report
	
	# generic components
	#unitNames,nameUnits = defineUnits([set(id for ns,id in namespaceID.iteritems() if ns not in {'label','description','utype'})],maxdist)
	#reportResults("omnicore",unitNames,nameUnits)
	
	# non-protein components
	#unitNames,nameUnits = defineUnits([set(id for ns,id in namespaceID.iteritems() if ns not in {'label','description','utype','ensembl_pid','refseq_pid','uniprot_pid'})],maxdist)
	#reportResults("non-protein",unitNames,nameUnits)
	
	# dbID components
	#unitNames,nameUnits = defineUnits([set(id for ns,id in namespaceID.iteritems() if ns not in {'label','description','utype','ensembl_pid','refseq_pid','uniprot_pid','symbol'})],maxdist)
	#reportResults("dbID",unitNames,nameUnits)
	
	# ccds components
	#unitNames,nameUnits = defineUnits([{namespaceID['ccds_gid']}],maxdist)
	#reportResults("CCDS",unitNames,nameUnits)
	
	diffNE = True
	
	if not diffNE:
		# ensembl components
		unitNames,nameUnits = defineUnits([{namespaceID['ensembl_gid']}],maxdist)
		reportResults("Ensembl",unitNames,nameUnits,report)
		
		# hgnc components
		unitNames,nameUnits = defineUnits([{namespaceID['hgnc_gid']}],maxdist)
		reportResults("HGNC",unitNames,nameUnits,report)
		
		# ncbi/entrez components
		unitNames,nameUnits = defineUnits([{namespaceID['entrez_gid']}],maxdist)
		reportResults("NCBI /<br>Entrez",unitNames,nameUnits,report)
		
		# N,E components
		unitNames,nameUnits = defineUnits([{namespaceID['entrez_gid']},{namespaceID['ensembl_gid']}],maxdist)
		reportResults("N,E",unitNames,nameUnits,report)
		
		# E,N components
		unitNames,nameUnits = defineUnits([{namespaceID['ensembl_gid']},{namespaceID['entrez_gid']}],maxdist)
		reportResults("E,N",unitNames,nameUnits,report)
	#if not diffNE
	
	# analyze E+N simultaneous component breakdown
	unitNames,nameUnits = defineUnits([{namespaceID['entrez_gid'],namespaceID['ensembl_gid']}],maxdist)
	reportResults("E+N",unitNames,nameUnits,report)
	
	if diffNE:
#		analyze_starfish(unitNames, nameUnits)
		analyze_units(unitNames)
		sys.exit(1)
	else:
		# C,H,N,E components
		#unitNames,nameUnits = defineUnits([{namespaceID['ccds_gid']},{namespaceID['hgnc_gid']},{namespaceID['entrez_gid']},{namespaceID['ensembl_gid']}],maxdist)
		#reportResults("C,H,N,E",unitNames,nameUnits)
		
		# H,N,E components
		unitNames,nameUnits = defineUnits([{namespaceID['hgnc_gid']},{namespaceID['entrez_gid']},{namespaceID['ensembl_gid']}],maxdist)
		reportResults("H,N,E",unitNames,nameUnits,report)
		
		# E,N,H,C components
		#unitNames,nameUnits = defineUnits([{namespaceID['ensembl_gid']},{namespaceID['entrez_gid']},{namespaceID['hgnc_gid']},{namespaceID['ccds_gid']}],maxdist)
		#reportResults("E,N,H,C",unitNames,nameUnits)
		
		# E,N,H components
		unitNames,nameUnits = defineUnits([{namespaceID['ensembl_gid']},{namespaceID['entrez_gid']},{namespaceID['hgnc_gid']}],maxdist)
		reportResults("E,N,H",unitNames,nameUnits,report)
		
		# analyze C+E+H+N simultaneous component breakdown
		#unitNames,nameUnits = defineUnits([{namespaceID['ccds_gid'],namespaceID['ensembl_gid'],namespaceID['hgnc_gid'],namespaceID['entrez_gid']}],maxdist)
		#reportResults("C+E+H+N",unitNames,nameUnits)
		
		# analyze E+H+N simultaneous component breakdown
		unitNames,nameUnits = defineUnits([{namespaceID['ensembl_gid'],namespaceID['hgnc_gid'],namespaceID['entrez_gid']}],maxdist)
		reportResults("E+H+N",unitNames,nameUnits,report)
	#if diffNE
	
	# report footer?
	if report:
		report.write("""
	</tbody>
</table>
""")
#foreach maxdist

if report:
	report.write("""
Graphs depict the mapping of identifiers (top) to units defined by a certain algorithm (left).<br>
<br>
The (all pairs) column shows adherence to the provided name equivalencies between all types.<br>
The (EHN pairs) column shows adherence to the provided name equivalencies between only Ensembl, HGNC and NCBI/Entrez IDs.<br>
The (EN pairs) column shows adherence to the provided name equivalencies between only Ensembl and NCBI/Entrez IDs.<br>
<br>
orange: ambiguous (identifier matches multiple units)<br>
teal: redundant (distinct identifiers match the same unit)<br>
green: unique (one identifier matches one unit)<br>
red: mismatched (the equivalent identifiers match different units)<br>
blue: unmatched (identifier does not match any unit)<br>
<br>
Hover the mouse over any graph to display the numeric details.<br>
</body>
</html>
""")
	report.close()
#if report

