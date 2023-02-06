import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from findgits import get_folder_list

class MainClass():
	def __init__(self):
		self.builder = Gtk.Builder()
		gladefile = "/home/kth/dotfiles/mainwin.glade"
		self.builder.add_from_file(gladefile)
		self.builder.connect_signals(self)

		self.treeone = self.builder.get_object('tree1')
		self.treeone_listStore = Gtk.ListStore(int, str)

		self.setup_treeview()

		win = self.builder.get_object('mainwin_id')
		win.connect('delete-event', Gtk.main_quit)
		win.show()

	def setup_treeview(self):
		renderer = Gtk.CellRendererText()
		pathColumn = Gtk.TreeViewColumn('Path', renderer, text=0)
		idColumn = Gtk.TreeViewColumn('Id', renderer, text=0)
		self.treeone.append_column(idColumn)
		self.treeone.append_column(pathColumn)
		self.treeone.set_model(self.treeone_listStore)

	def load_treeone(self):
		folders = get_folder_list('/home/kth/development2/games/quakestuff')
		for idx,k in enumerate(folders):
			self.treeone_listStore.append([idx, str(k)])

	def on_button_one_clicked(self, widget):
		print('loadpaths....')
		self.load_treeone()

class MainWindow(Gtk.Window):
	def __init__(self):
		Gtk.Window.__init__(self, title="Hello World")
		self.button = Gtk.Button(label="Click Here")
		self.button.connect("clicked", self.on_button_clicked)

		self.connect('delete-event', Gtk.main_quit)

		self.set_border_width(5)
		self.set_position(Gtk.WindowPosition.CENTER)
		self.vbox = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=5)
		self.add(self.vbox)

		self.hbox1 = Gtk.Box(spacing=5)
		self.add(self.hbox1)

		self.label = Gtk.Label(label="Hello World", xalign=0)
		self.hbox1.pack_start(self.label, True, True, 0)
		self.hbox1.pack_start(self.button, False, False, 0)

		self.vbox.pack_start(self.hbox1, True, True, 0)

	def on_button_clicked(self, widget):
		print("Hello World")

if __name__ == '__main__':
	mc = MainWindow()
	mc.show_all()
	Gtk.main()
