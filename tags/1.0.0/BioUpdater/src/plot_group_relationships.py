#!/usr/bin/env python
# To change this template, choose Tools | Templates
# and open the template in the editor.
import sqlite3, sys, os
import pydot
__author__="torstees"
__date__ ="$Feb 15, 2011 11:16:48 AM$"



class Plot:
	def __init__(self, id, db, depth=3):
		self.id				= id
		self.db				= db
		self.depth			= depth
		self.LoadGroups()
		self.g				= pydot.Dot()
		self.links			= dict()
		self.nodes			= set()

	def AddDisease(self, id):
		if int(id) in self.groupIDs:
			n					= pydot.Node(int(id))
			n.set_shape("box")
			n.set_label("""<
<TABLE BGCOLOR="antiquewhite2">
  <TR><TD >%s</TD></TR>
  <TR><TD VALIGN="bottom" ALIGN="right">%s</TD></TR>
  <TR><TD >%s</TD></TR>
  <TR><TD>%s</TD></TR>
</TABLE>>"""  % ("Disease", self.groupIDs[int(id)], id, len(self.groupRelations[int(id)])))
			self.g.add_node(n)

	def AddDrug(self, id):
		if int(id) in self.groupIDs:
			n					= pydot.Node(int(id))
			n.set_shape("box")
			n.set_label("""<
<TABLE BGCOLOR="bisque">
  <TR><TD >%s</TD></TR>
  <TR><TD VALIGN="bottom" ALIGN="right">%s</TD></TR>
  <TR><TD >%s</TD></TR>
  <TR><TD>%s</TD></TR>
</TABLE>>"""  % ("Drug", self.groupIDs[int(id)], id, len(self.groupRelations[int(id)])))

			self.g.add_node(n)
	def AddPathway(self, id):
		if id and int(id) in self.groupIDs:
			n					= pydot.Node(int(id))
			n.set_shape("box")
			n.set_label("""<
<TABLE BGCOLOR="azure3">
  <TR><TD >%s</TD></TR>
  <TR><TD VALIGN="bottom" ALIGN="right">%s</TD></TR>
  <TR><TD >%s</TD></TR>
  <TR><TD>%s</TD></TR>
</TABLE>>"""  % ("Disease", self.groupIDs[int(id)], id, len(self.groupRelations[int(id)])))
			self.g.add_node(n)

	def AddComplex(self, node):
		if id and int(id) in self.groupIDs:
			components = []
			for component in self.groupRelations[int(id)]:
				components.append("""<TR BGCOLOR="%s"><TD>%s</TD><TD>%s</TD></TR>""" % (component[0], component[1], ""))


			n					= pydot.Node(int(id))
			n.set_shape("box")
			label = """<
<TABLE BGCOLOR="bisque">
  <TR><TD COLSPAN="2">%s</TD>
      <TD BGCOLOR="darkolivegreen2"
          VALIGN="bottom" ALIGN="right">%s</TD></TR>
  <TR><TD BGCOLOR="darkolivegreen2">%s</TD></TR>
</TABLE>
<TABLE BGCOLOR="white">
%s
</TABLE>>"""  % (self.groupIDs[int(id)], id, "-", "\n".join(components))

			n.set_label(label)
			self.g.add_node(n)


	def AddNode(self, node):
		if node[0] == None:
			Iwonder
		id = int(node[0])

		if id not in self.nodes:
			if int(id) in self.groupRelations:
				if len(self.groupRelations[id]) > 20:
					self.AddComplex(id)
					return
			if node[1] == "Disease":
				self.AddDisease(id)
			elif node[1] == "Drug":
				self.AddDrug(id)
			else:
				self.AddPathway(id)
			self.nodes.add(id)

	def DoubleCheck(self, l, r):
		if l in self.links:
			return r in self.links[l]
		return False
	
	def AddEdge(self, source, dest):
		if source[0] and dest[0]:
			if self.DoubleCheck(int(source[0]), int(dest[0])) or self.DoubleCheck(int(dest[0]), int(source[0])):
				return
		if source[0] == dest[0]:
			return

		if source[0] and dest[0]:
			self.AddNode(source)
			self.AddNode(dest)
			if int(source[0]) not in self.links:
				self.links[int(source[0])] = []
			self.links[int(source[0])].append(int(dest[0]))
			if int(dest[0]) not in self.links:
				self.links[int(dest[0])] = []
			self.links[int(dest[0])].append(int(source[0]))

			self.g.add_edge(pydot.Edge(int(source[0]), int(dest[0])))

	def LoadGroups(self):
		"""Attempts to load all group data (and relationships) from the database which have the same group_type_id as is passed via id"""
		self.groupIDs			= dict()
		self.groupRelations	= dict()

		c						= self.db.cursor()
		c.execute("SELECT group_type_id FROM groups WHERE group_id=%s" % (id))
		groupTypeID			= c.fetchone()[0]
		c.execute("SELECT group_id, group_name FROM groups WHERE group_type_id=%s" % (groupTypeID))
		for row in c.fetchall():
			#groupIDs.add(row[0])
			self.groupIDs[row[0]] = row[1]
		print len(self.groupIDs), " Groups Loaded"

		c.execute("SELECT parent_id, child_id, relationship FROM group_relationships WHERE parent_id IN (%s)" % (",".join(["%s" % a for a in self.groupIDs])))

		for row in c.fetchall():
			if int(row[0]) not in self.groupRelations:
				self.groupRelations[int(row[0])]		= []
			self.groupRelations[int(row[0])].append((int(row[1]), row[2]))

	def RenderNodes(self, source, nodes, depth):
		if depth < 1:
			return
		print "Rendering Nodes: ", source, len(nodes), " Depth(%s)" % (depth)
		for node in nodes:
			#this needs to be changed to switch according to type, but I don't think that information is present
			self.AddNode(node)
			if int(node[0]) in self.groupRelations:
				self.RenderNodes(node, self.groupRelations[int(node[0])], depth-1)
				if source:
					if len(self.groupRelations[int(node[0])]) < 21:
						for dest in self.groupRelations[int(node[0])]:
							self.AddEdge(source, dest)

	def RenderPlot(self):
		filename					= "bio-%s" % (self.groupIDs[int(id)].split()[0].split(",")[0])
		self.RenderNodes((None, ""), [(self.id, "")], self.depth)
		self.g.write("%s.gv" % (filename), "dot", "raw")
		#print self.g.to_string()
		print len(self.groupIDs), " Groups Loaded"

		self.g.write("%s.png"%(filename), "dot", "png")

		os.system("open %s" % ("%s.png"%(filename)))
	

if __name__ == "__main__":
	if len(sys.argv) > 2:
		db					= sqlite3.connect(sys.argv[1])
		groupIDs			= sys.argv[2:]

		for id in groupIDs:
			p				= Plot(id, db)
			p.RenderPlot()
	else:
		print>>sys.stderr, "Usage: db group_id [group_id..]"
		print>>sys.stderr, "\tThis assumes you already know group_ids, which are the unique identifiers. Biofilter can list these for you, should you need."
