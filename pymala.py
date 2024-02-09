import re
import sys
import glob
from time import time, sleep
from os import path, listdir, chdir, getcwd
from multiprocessing import Process, Queue, cpu_count, active_children

class PymalaReader:
    """Defines a virtual xml (or html) file that may comprise of multiple files within a directory sharing
    a given template. The next method iterates through this virtual xml file (html) returning sections 
    defined by one ore more specific root tags. These sections are returned as already cleaned Pymala objects,
    to be further parsed.
    The PymalaReader allows to indiscriminately handle different delivery forms of *ml data, be it one single
    file with multiple entities, one file per entity or a mix of both."""

    def __init__(self, template, root = None, chunk = 0, encoding = 'utf-8'):
        """Defines which files should be included as xml or html stream. The path template may contain '*' or 
        '?' placeholders for any number respectively any single character. All files matching the template 
        will be included. By default, all files are considered to contain a single document entity. In case, a 
        file comprises multiple entities, you have to specifiy the root parameter.
        The root defines tags that separate the entities within the document stream. If necessary, multiple 
        alternative tags can be defined separated by the '|' character. Tag definitions may contain '*' and 
        '?' placeholders, but not the tag enclosings '<' and '>' (for more information, see Pymala.tags 
        method). A document entity is extracted from the stream beginning from the root tag until the 
        corresponding closing tag (</...>).
        The PymalaReader is ready for multi-processing by using a synchronized queue to store the files
        matching the template. In case the number of files is lower than the processes, you can separate
        larger files into virtual chunks to accomodate multi-processing. This requires a distinctive root.
        The chunk size can be specified in MBs, i.e. chunk = 16 separates a larger file into several 16 MB 
        chunks. Every process handles a chunk by opening the file, jumping to the start position, 
        searching for a root tag and is corresponding close tag to report the pymala entities. It continues 
        until the next pymala entity would start in the adjacent chunk. Operating with chunks is not suitable
        when every file is representing a single entity."""
        self.buffer = 131072 # 128kB
        self.template = template
        self.chunk = chunk
        self.end_of_chunk = False
        self.encoding = encoding
        self.file = None
        self.root = None
        self.end = -1
        self.pymalas = Queue()
        if root:
            self.root = Pymala()
            self.root.tags(root)
        files = glob.glob(template)
        if self.root and self.chunk > 0:
            chunk = int(self.chunk*1048576)
            for f in files:
                size = path.getsize(f)
                chunks = max(int(size / chunk) - 1, 0)
                start = 0
                for c in range(chunks):
                    stop = start + chunk
                    self.pymalas.put((f, start, stop))
                    start = stop
                self.pymalas.put((f, start, -1))
        else:
            for f in files:
                self.pymalas.put((f, 0, -1))
        self.pymalas.put(None) # end of queue
   
    def next(self):
        """Retrieve the next entity from the xml (html) stream according to the template and root settings."""
        if self.end_of_chunk: self.__close()
        if not self.file:
            if not self.__open(): return None
            if not self.root:
                pymala = self.file.read().decode(self.encoding)
                self.__close()
                if pymala: return Pymala(pymala)
                return None
            self.root.reset(self.__read(True))
        while self.file:
            while self.root.pymala:
                if self.root.find(): break
                self.root.reset(self.__read(True))
            if self.root.pymala and self.root.tag: break
            self.__close()
            if not self.__open(): return None
            self.root.reset(self.__read(True))
        pymala = self.root.tag
        op_tag = self.root.tag[1:-1].lstrip().partition(' ')[0] ## remove tags '<>' and pick the first word
        if op_tag.startswith('/'): cl_tag = op_tag.lstrip('/')
        else: cl_tag = '/'+op_tag
        op = self.root.copy()
        op.tags(op_tag)
        cl = op.copy()
        cl.tags(cl_tag)
        cl.begin = cl.pos
        ballance = 1
        while cl.pymala:
            if cl.find():
                op.end = cl.pos
                while op.find():
                    ballance += 1
                    pymala += cl.pymala[cl.begin:op.pos]
                    cl.begin = op.pos
                ballance -= 1
                pymala += cl.pymala[cl.begin:cl.pos]
                cl.begin = cl.pos
                if ballance == 0:
                    self.root.pymala = cl.pymala
                    self.root.begin = cl.begin
                    self.root.end = cl.end
                    self.root.pos = cl.pos
                    return Pymala(pymala)
                op.pos = cl.pos
                op.end = cl.end
                continue
            while op.find():
                ballance += 1
            pymala += cl.pymala[cl.begin:]
            cl.reset(self.__read(False))
            op.reset(cl.pymala)
        self.__close()
        return Pymala(pymala)

    def size(self):
        """Returns the queue size without the stop element."""
        return self.pymalas.qsize() - 1

    def __del__(self):
        """Destructor closes the current open file."""
        if self.file: self.file.close()

    def __close(self):
        """Closes the open document file. The next call of the next method will open a new one.
        Also, this is the place for future clean-up proceedings.""" 
        self.file.close()
        self.file = None
    
    def __open(self):
        """Gets the next file item from the queue, opens it and moves the file pointer to the start position.
        A file can be opened multiple times if the PymalaReader is used in a muliprocessing context."""
        self.file = None
        self.end_of_chunk = False
        file = self.pymalas.get()
        if file == None: 
            self.pymalas.put(None)
            return False
        file, begin, self.end = file
        self.file = open(file, "rb")
        if begin > 0: self.file.seek(begin)
        return True
    
    def __read(self, open):
        """Reads the next buffer section of the current file by keepin tags intact.
        As a section is never allowed to end within a tag, it may be larger. A section always ends at the end of a
        tag, before a tag or after a line feed as tags have to be inline.
        When open == True, the reader is looking for open root tags, otherwise it closes an open root tag.
        The reader cannot search for open root tags beyond the chunk limit.
        The end parameter declares a chunk boundary that cannot be crossed except to complete a tag."""
        buffer = self.buffer
        if self.end > 0:
            buffer = min(self.end - self.file.tell(), buffer)
            if buffer <= 0:
                self.end_of_chunk = True
                if open: return ''
                buffer = self.buffer
        chunk = self.file.read(buffer)
        rest = b''
        chr = self.file.read(1)
        while chr:
            if chr in b'<>\n':
                if chr == b'<': self.file.seek(-1, 1)
                else: rest += chr
                break
            rest += chr
            chr = self.file.read(1)
        chunk += rest
        return chunk.decode(self.encoding)

class PymalaPath:
    """Transforms the tree structure of a Pymala document into a rectangular table. The data for the columns
    is addressed by paths leading through the XML structure. The table structure can be defined with a PymalaTable 
    object. Paths can be added with the add method. The collect method performs the transformation."""

    def __init__(self, data = None):
        """Links a PymalaTable object to define the structure, i.e. order of fields, combined fields, 
        field names, of the resulting table (see PymalaPath.collect). If none is specified, 
        every path name will constitute a column in the order of path definitions (see PymalaPath.add)."""
        self.root = []
        self.paths = {}
        self.data = None
        self.sep = ","
        if not data: self.data = PymalaTable()
        else:
            if isinstance(data, str): self.data = PymalaTable(data)
            elif isinstance(data, PymalaTable): self.data = data
            else: raise TypeError("invalid data parameter")

    def add(self, path_or_root = ''):
        """Adds a new path to the PymalaPath structure. The path parameter has the following syntax:
        
        <path_name> = <tag_definition>[.<tag_definition> ...][:<property_name>]
        
        The tag definitions correspond with the parameter of the Pymala.tags method. The path name has 
        to be unique. By default, a path has to follow a hierachical order diving deeper into the 
        document structure with every consecutive tag. By default tags are separated by a dot. In case 
        dots are a part of the tag specification or you just do not like dots, the greater-than sign ">" 
        can be used as alternative tag separator for the whole definition, which also switches the 
        property designator to the lesser-than sign '<'. In case several tags have to be skipped that 
        carry no relevant structural information, a single star * character as tag definition can be 
        used. If just one level needs to be skipped regardless of the tag, the definition *? accepts 
        just any tag without skipping multiple tags. Usually, a path starts with a * tag definition to 
        skip directly to the relevant structure, e.g.:
        
        pp.add("client_name = *.clientlist.client|customer.name")
        
        If the path name is skipped, a path root will be defined. You can keep the equal sign for clarity or 
        omitt it. All following path definitions start from this root as if it is preceding the path, e.g.:
        
        pp.add("*.clientlist") # or # pp.add("= *.clientlist")
        pp.add("client_name = client|customer.name")
        
        which is equivalent to:
        
        pp.add("client_name = *.clientlist.client|customer.name")
        
        To reset the root just, specify any character of this list ["=",".",":","<",">"], an empty root or no 
        parameter at all. To continue a root, the definition has to start with a path separator, i.e. 
        ".client|customer" will extend the root by this tag. This useful when tag structures are deeply 
        staggered and the root definition becomes unwieldy. You can change the root at any time, but keep in 
        mind that only the following path definitions are affected. While a root definitions may not have 
        properties, a path definition starting with a colon can address properties of the root."""
        path = path_or_root.partition('=')
        if not path[1]: path = ('', '=', path[0])
        name = path[0].strip().lower()
        path = path[2].strip()
        if '>' in path or '<' in path:
            sep_path = '>'
            sep_prop = '<'
        else:
            sep_path = '.'
            sep_prop = ':'
        if path in ['','>','<','.',':']:
            self.root = []
            return
        cont = path.startswith(sep_path)
        path = path.split(sep_path)
        path = [p for p in path if p != '']
        prop = ''
        if path:
            path = [path[0]] + [path[p] for p in range(1,len(path)) if path[p] != '*' or path[p-1] != '*'] # skipping '*' sequences
            prop = path[-1].split(sep_prop, 1)
            if not prop[0]: path.pop()
            else: path[-1] = prop[0]
            prop = prop[1] if len(prop) > 1 else ''
        if not name:
            if prop: raise SyntaxError(f"properties are not allowed in root definitions: {path_or_root}")
            if cont: self.root += path
            else: self.root = path
            return
        path = self.root + path
        if not path: raise SyntaxError(f"empty path definition: {path_or_root}")
        if name in self.paths: raise SyntaxError(f"duplicate path name: {path_or_root}")
        elif not name.isidentifier(): raise SyntaxError(f"invalid path name: {path_or_root}")
        if prop: path.append("<"+prop) # property is like a special tag
        column = self.data.register(name)
        self.paths[name] = (path, column)

    def missing(self):
        return [name for name in self.data.table if not name in self.paths] 

    def header(self):
        """Returns the tab delimited header."""
        return self.data.output_header()

    def collect(self, pymala):
        """Collects the contents of the paths within the Pymala object returning a tab delimited table
        as a list. Every element represents a line of the table.
        The structure of the table is defined by the linked PymalaTable object.""" 
        root = {None: ([(pymala, {})], [])}
        for path, column in self.paths.values():
            self.__expand(root[None], path, column, 0)
        for column in self.data.table.values(): column.clear() # reseting without changing the id
        self.__collect(root, {})
        return self.data.output_data()

    def __expand(self, root, path, column, pos):
        """Recursively expands the PymalaPath tree root by root with the corresponding path elements."""
        branches, data = root
        if pos >= len(path):
            data.append(column)
            return
        tag = path[pos]
        if tag == '*' and pos+1 < len(path):
            self.__expand(root, path, column, pos+1)
            return
        find = pos > 0 and path[pos-1] == "*"
        for pymala, branch in branches:
            twig = branch.get(tag, None)
            if twig == None:
                if tag.startswith('<'): # virtual property tag refers to the same pymala object as the inclusive tag
                    twig = (pymala, []) # no further branching possible
                else:
                    twig = []
                    if pymala == None:
                        twig += (None, {}),
                    else:
                        pymala.reset() # back to the begin of the document section reseting the root tag
                        pymala.tags(tag)
                        while self.__browse(pymala, find):
                            twig += (pymala.extract(), {}),
                            find = False
                        if not twig: twig += (None, {}),
                    twig = (twig, [])
                branch[tag] = twig
            self.__expand(twig, path, column, pos+1)

    def __collect(self, root, properties):
        """Recursively collects the data within the tags at the data nodes of the paths. When a property is 
        defined, it will be collected instead. Data nodes of a path do not have to be neccessarily at the end
        of a branch within the tree structure as a path may be part of a longer path definition.
        By ermerging from a lower levels, the data will be rectanglified to maintain a rectangular table shape.
        The properties parameter loops a dictionary through the recursive call hierarchy collecting the
        properties for recurring extractions from the same tag. Otherwise, the property dictionary would have
        been inefficiently created for every requested property."""
        rectangle = []
        for tag, branching in root.items():
            if tag and tag.startswith('<'):
                pymala, columns = branching
                rectangle += columns
                value = ''
                if not pymala == None:
                    prop = properties.get(id(pymala), None)
                    if prop == None:
                        pymala.reset()
                        prop = pymala.properties()
                        properties[id(pymala)] = prop
                    value = self.__properties(prop, tag[1:])
                for column in columns:
                    column.append(value)
            else:
                branches, columns = branching
                rectangle += columns
                for column in columns:
                    for pymala, branch in branches:
                        if pymala == None:
                            column.append('')
                        else:
                            pymala.reset()
                            column.append(self.sep.join(pymala.collect()))
                columns = None
                for branch in [branch for pymala, branch in branches if branch]:
                    columns = self.__collect(branch, properties)
                    self.__rectanglify(columns)
                if columns: rectangle += columns
        return rectangle

    def __properties(self, properties, tag):
        val = properties.get(tag, None)
        if not val == None: return val
        if not ('*' in tag or '|' in tag or '?' in tag): return ''
        templates = [re.compile(like_to_regex(t)) for t in tag.split('|')]
        values = []
        for key, val in properties.items():
            for t in templates:
                if t.match(key):
                    values += val,
                    break
        return '|'.join(values)
        
    def __browse(self, pymala, find):
        """Allows to alternate between find and browse mode to locate the next tag. Required if the path
        contains wildcard elements (a single *)."""
        if find: return pymala.find()
        return pymala.browse()

    def __rectanglify(self, columns):
        """Fills data lists in the table with the respectively last element up to the length of the longest.
        Repeated values have the same address (id) as the originating element. This is important to identify
        repeated values (see PymalaTable.output_data)."""
        maxlen = max([len(item) for item in columns]) if columns else 0
        for v in columns:
            if len(v) != maxlen:
                last = [v[-1]] if v else ['']
                v += last * (maxlen-len(v))

class PymalaTable:
    """A PymalaTable consists of a header definition and the associated data.
    When linked to a PymalaPath, it will determine the structure of the parsed data. Every path name not defined
    in the PymalaTable template will be appended to the template definition as a new field. Name conflicts
    will be resolved automatically.
    A table template consists of field definitions separated by commas. Every field represents a column
    in the table. A field may comprise of multiple path names (see PymalaPath):
    Syntax: [<column_name> = ]<field_template>[,[<column_name> = ]<field_template> ...]
    field_template: {<string> | [!]<path_name>[.<pos>]}[<string> | [!]<path_name>[.pos] ...]
    string: {"<txt_without_double_quotes>" | '<txt_without_single_quotes>'}
    
    Example: !id, fullname = name, birthdate = year "." month "." day, gender, first = job.1, second = job.2
    
    An exclamation mark before a path name declares a key field. If a key field is empty the whole data line
    will be suppressed. If all additional data fields are empty, the line will also be suppressed. By default,
    the data will be represented as table with multiple rows to accomodate paths with multiple values. 
    If at least one path name is expanded with a position, only one line will be reported. The position
    denotes the row of the value."""
    
    def __init__(self, template = ""):
        """Creates the PymalaTable with an initial template."""
        self.template = []
        self.table = {}
        self.explicit = {}
        self.implicit = {}
        self.keys = []
        self.single = False
        self.append(template)

    def append(self, template):
        """Appends to an existing template."""
        fields = [[]]
        quote = False
        for field in self.__quote_split(template):
            if field.startswith('"'):
                fields[-1].append(field)
                quote = True
            else:
                items = [[item.strip().lower()] for item in field.split(',')]
                if quote:
                    fields[-1].append(items[0][0])
                    items.pop(0)
                fields += items
                quote = False
        for field in [list(filter(None, field)) for field in fields if list(filter(None, field))]:
            f = []
            name = ''
            before, sep, item = field[0].partition('=')
            header = self.implicit
            if sep == '=':
                name = before.strip()
                item = item.strip()
                if not name.isidentifier(): raise SyntaxError(f"invalid name definition: {field[0]}")
                if not item:
                    if len(field) == 1: raise SyntaxError(f"invalid name definition: {field[0]}")
                    field.pop(0)
                else: field[0] = item
                header = self.explicit
            for item in field:
                if item.startswith('"'): f.append(item[1:-1])
                else:
                    before, sep, after = item.partition('.')
                    pos = 0
                    if sep == '.':
                        if not after.isdecimal(): raise SyntaxError(f"invalid field definition: {item}")
                        pos = int(after)-1
                        self.single = pos >= 0
                    key = False
                    if before.startswith('!'):
                        before = before[1:]
                        key = True
                    if not before.isidentifier(): raise SyntaxError(f"invalid field definition: {item}")
                    if not name: name = before
                    column = self.table.setdefault(before, [])
                    f.append((column, pos, key))
                    if key: self.keys.append(column)
            self.template.append(f)
            header.setdefault(name, []).append(len(self.template)-1)
    
    def register(self, path_name):
        """Registers a single path name and returns the asscoiated data column. When it does not already
        exist, it will be appended to the template as a new field."""
        path_name = path_name.strip().lower()
        if not path_name in self.table: self.append(path_name)
        return self.table[path_name]

    def output_header(self):
        """Returns the header as a list item."""
        return '\t'.join(self.__assemble_header())

    def output_data(self):
        """Returns the tab separated data as a list. Every element represents a line of data."""
        out = []
        maxlen = len(next(iter(self.table.values())))
        if self.single: lines = [0] + [i for i in range(1,maxlen) if [k for k in self.keys if k[i] != k[i-1]]]+[maxlen]
        else: lines = [i for i in range(maxlen+1)]
        lines = [(lines[i], lines[i+1]) for i in range(len(lines)-1)]
        for start, end in lines:
            line = []
            data = 0
            datacnt = 0
            keycnt = 0
            for field in self.template:
                content = ''
                has_const = False
                has_data = False
                needs_data = False
                for item in field:
                    if isinstance(item, tuple):
                        needs_data = True
                        column, pos, key = item
                        if not key: data = 1
                        index = start + pos if pos > 0 else start
                        if index >= end or pos != 0 and index > 0 and id(column[index]) == id(column[index-1]): # no repeated values for positional fields
                            value = ''
                        else:
                            value = column[index]
                            value = value.strip().replace('\t', "\\t").replace('\r\n', "\\n").replace('\n', "\\n").replace('\r', "\\n").replace('&amp;', '&').replace('&gt;', '>').replace('&lt;', '<')
                        if value:
                            has_data = True
                            if key: keycnt += 1
                            else: datacnt += 1
                        content += value
                    else: 
                        has_const = True
                        content += item
                if has_const and needs_data and not has_data: line.append('') # suppress literals in composed field without data
                else: line.append(content)
            if keycnt == len(self.keys) and datacnt >= data: out.append('\t'.join(line))
        return out

    def __quote_split(self, str):
        """Slits a string into a list with every element either containing a literal or a string
        always enclosed in double quotes."""
        splits = []
        while str:
            quote = '"' 
            pos = str.find("'")
            if pos >= 0 and str.find('"', 0, pos) < 0: quote = "'"
            triplet = str.split(quote, 2)
            if len(triplet) == 1: 
                if triplet[0]: splits += [triplet[0]]
                break
            if len(triplet) == 2: raise SyntaxError("invalid quotes")
            if triplet[0]: splits += [triplet[0]]
            splits += ['"'+triplet[1]+'"']
            str = triplet[2]
        return splits

    def __assemble_header(self):
        """Returns a list with column headers by resolving name conflicts giving explicitly declared names
        the preference."""
        header = [''] * len(self.template)
        reserved = set(self.explicit)
        conflict = set()
        for name, cols in self.explicit.items():
            start = 0
            for pos in cols:
                header[pos], start = self.__resolve_name(name, start, conflict, reserved)
        reserved = reserved.union(set(self.implicit))
        for name, cols in self.implicit.items():
            start = 0 if name and len(cols) == 1 else 1
            for pos in cols:
                header[pos], start = self.__resolve_name(name, start, conflict, reserved)
        return header

    def __resolve_name(self, name, start, conflict, reserved):
        """Creates a unique column name by resolving name conflicts."""
        n = name if start <= 0 else name+"_"+str(start)
        if n in conflict:
            while n in conflict or n in reserved:
                start += 1
                n = name+'_'+str(start)
        conflict.add(n)
        return (n, start+1)

class Pymala:
    """The Pymala class facilitates simple parsing methods for html or xml document strings.
    The class relies on simple tag searches and an internal postioning mechanism. It is intended to extract 
    specific data from these documents without the need to create complex tree structures of the documents 
    beforehand. Pymala sub-documents can be extracted without actually creating copies of the original string.
    The class always tries to prevent the creattion of data copies. Extractions are handles via positional
    specifications.
    Public attributes:
    root - Root tag of extraction (omitted in document)
    tag - Last tag encountered.
    pos - Current position within the xml/html string. Can be set to 0 to reset the parsing.
    begin - Start position of the current pymala object in the pymala string
    end - End position of the current pymala object in the pymala string"""
    
    
    def __init__(self, document = ""):
        """Initializer takes a html or xml document as a string.""" 
        self.pymala = document
        self.root = ""
        self.look = {}
        self.like = ""
        self.tag = ""
        self.pos = 0
        self.begin = 0
        self.end = len(self.pymala)
    
    def reset(self, document = None):
        """Resets the parsing to the beginning of the document string.
        Current tag is set to the root tag of the extraction."""
        if document != None:
            self.pymala = document
            self.begin = 0
            self.end = len(self.pymala)
            self.root = ""
        self.pos = self.begin
        self.tag = self.root

    def clean(self):
        """Removes leading and trailing whitespace characters within tags.
        If the object is an extraction, this will create a separate pymala string.
        This function is quite slow and not really necessary. It is better to
        clean only the extracted data than the whole document."""
        if self.begin > 0: self.pymala = self.pymala[self.begin:self.end]
        self.pymala = self.pymala.replace('\t', ' ')
        shatter = [item.split('<') for item in self.pymala.split('>')]
        clean = []
        for gt in shatter:
            clean.append('<'.join([item.strip() for item in gt]))
        self.pymala = '>'.join(clean)
        self.reset(self.pymala)
        return self
        
    def copy(self, deep = False):
        """Returns a copy of the object. In case of a deep copy, a copy of the pymala document will be created
        reseting begin, end and current position accordingly."""
        new = Pymala()
        for p in filter(lambda x : not x.startswith('_') and not callable(getattr(self, x)), dir(self)):
            setattr(new, p, getattr(self, p))
        if deep:
            new.pymala = new.pymala[new.begin:new.end]
            new.pos = new.pos - new.begin
            new.begin = 0
            new.end = len(new.pymala)
        return new
    
    def tags(self, like):
        """Defines the tags to look for using the self.find() method. The like parameter can consist of multiple 
        definitions separated by the pipe "|" character. A definition may contain placeholders: "*" for 
        any number of characters and "?" for any single character. Definitions can also refer to attributes/properties 
        within the tag. A tag definition is always open to the right as long as it is separated by a whitespace from the
        tailing rest. Do not use enclosing <> tag characters for the defintions!
        Example 1: customer|client
                   The tag should designate a customer or a client.
        Example 2: client*status*=*deleted*
                   Looks for the next client with status "deleted".
        Example 3: client_no_??*
                   Only clients with at least a 2 additional characters (most likely digits) are selected.
        The function returns a dictionary. The keys are the parts of the tags until the first placeholder while the 
        values are lists of regular expression for the whole definition. This setup allows for efficient retrieval of 
        multiple tags."""
        self.like = like
        self.look = self.__look(like)
        return self.look
        
    def find(self, like = None):
        """Searches for the next tag fitting the tag definition of the like parameter. If this parameter is omitted, 
        it uses the definitions of the previous self.tags() call. Every call of the method progresses through the 
        document according to the tag definitions. It returns the tag or an empty string when none of the defined 
        tags could be found. If the search was not successful, the internal position will not be affected."""
        if like and like != self.like: self.tags(like)
        self.pos, self.tag = self.__find(self.look, self.pos, self.end)
        return self.tag
    
    def browse(self, like = None):
        """Browses for the next tag fitting the tag definition of the like parameter while staying on the current
        level of the document structure. If the like parameter is omitted, it uses the definitions of the previous
        self.tags() call. It returns the tag or an empty string when none of the defined tags could be found on the
        current level. If the browse was not successful, the internal position will not be affected. To browse through
        all tags on the current level, you have to call extract after every call of browse."""
        if like and like != self.like: self.tags(like)
        look = [item for sub in self.look.values() for item in sub]
        pos = self.pos
        while pos < self.end:
            tag, pos = self.__next(pos)
            if not tag: return ''
            for exp in look:
                if exp.match(tag):
                    self.pos = pos
                    self.tag = tag
                    return tag
            pos = self.__extract(tag, pos, self.end) # skip all deeper tags to stay in level
        return ""

    def next(self):
        """Returns the next tag while progressing through the document. If there are no tags left, it returns 
        an empty string."""
        self.tag, self.pos = self.__next(self.pos)
        return self.tag

    def extract(self, progress = True):
        """Extracts a section from the document enclosed by the current tag.
        The sections starts right after the tag and encloses the corresponding closing tag.
        The section will be returned as new Pymala object referencing the original document but with
        other boundaries. By default, the internal position will be progressed after the extraction.""" 
        new = self.copy()
        new.root = self.tag
        new.begin = self.pos
        new.pos = self.pos
        if not self.tag:
            new.pos = new.end
            return new
        new.end = self.__extract(self.tag, new.pos, new.end)
        if progress: self.pos = new.end
        return new

    def properties(self, tag = None):
        """Returns a dictionary of all the property names as keys referring the attribute values.
        When the tag parameter is omitted, the method will use the last tag encountered by the find(), browse() or 
        next() method."""
        props = {}
        if not tag: tag = self.tag
        tag = tag.strip().rstrip('>').rstrip().rstrip('/').rstrip().rstrip('?')
        tag += " x"  # dummy name
        frags = tag.split('=')
        name = frags[0].rstrip().split(' ')[-1]
        frags = frags[1:]
        open = None
        w = ""
        end = len(frags)-1
        for i in range(len(frags)):
            f = frags[i]
            if open == None:
                f = f.lstrip()
                if f[0] in "'\"" and not f[0] in f[1:] and i < end:
                    open = f[0]
                    w = f
                    continue
                else: w = f.rstrip()
            else:
                if not open in f and i < end:
                    w += "=" + f
                    continue
                else: w += "=" + f.rstrip()
            k = 0
            for j in range(len(w)-1, -1, -1):
                if not w[j].isidentifier(): 
                    k = j+1
                    break
            content = w[:k].rstrip('; ')
            if content[0] in "'\"" and content[-1] == content[0]: content = content[1:-1]
            if name in props: props[name] += '|'+content  # just in case properties are not unique
            else: props[name] = content
            name = w[k:]
            open = None
            w = ""
        return props

    def content(self):
        """Returns a the next content following the last tag until another tag is encountered without progressing
        through the document"""
        content, pos = self.__content(self.pos)
        return content

    def collect(self, until = None, empty = False):
        """Recursively collects all contents up to the valid close tag skipping interim tags. If the 
        until parameter is specified, its definition(s) will be used to contain the collection. See method tags
        for the syntax of the until parameter. The until tags are not resolved recursively. Usually, closing tags have 
        the "/" prefix. Empty contents are skipped by default but can be included. The document position will not be progressed.
        The contents are returned in a list."""
        pos = self.pos
        con = []
        if not until:
            if not self.tag or pos == self.begin:
                while pos < self.end:
                    c, pos = self.__content(pos)
                    if empty or c: con += [c]
                    tag, pos = self.__next(pos)
                    if not tag: return con
                return con
            return self.extract().collect()
        end = [item for sub in self.__look(until).values() for item in sub]
        while pos < self.end:
            c, pos = self.__content(pos)
            if empty or c: con += [c]
            tag, pos = self.__next(pos)
            if not tag: return con
            for exp in end:
                if exp.match(tag): return con
        return con

    def search(self, like):
        """Searches for the next content matching one of the content definitions of the like parameter. The like 
        parameter can consist of multiple definitions separated by the pipe "|" character. A definition 
        may contain placeholders: "*" for any number of characters and "?" for any single character. If found, the 
        internal document position will be progressed after the content, which will be returned."""
        like = [re.compile(like_to_regex(l)) for l in like.split('|')]
        pos = self.pos
        while pos < len(self.pymala):
            con, pos = self.__content(pos)
            for exp in like:
                if exp.match(con):
                    self.pos = pos;
                    return con
            tag, pos = self.__next(pos)
            if not tag: return ""
        return ""

    def __look(self, like):
        """Defines the tags to look for using the find() method. The like parameter can consist of multiple 
        definitions separated by the pipe "|" character. A definition may contain placeholders: "*" for any 
        number of characters and "?" for any single character. Definitions can also refer to attributes/properties 
        within the tag. The <> brackets are not allowed. A tag definition is always open to the right as long as it is 
        separated by a whitespace from the tailing rest.
        Example: client*status*=*deleted*|/clientlist
        Looks for the next client with status "deleted" or the end of the clientlist, i.e. </clientlist some stuff> 
        The function returns a dictionary. The keys are the parts of the tags until the first placeholder while the 
        values are lists of regular expression for the whole definition. This setup allows for efficient retrieval of 
        multiple tags."""
        look = {}
        for template in like.split('|'):
            if template.startswith('<') or template.endswith('>'): raise SyntaxError(f"invalid tag definition: {like}")
            template = '<'+template
            pos = list(filter(lambda x: x >= 0, [template.find(x) for x in "*?"]))
            pos = min(pos) if pos else len(template)
            key = template[:pos]
            if not template.endswith('*'): template = re.compile(like_to_regex(template) + '(\\s.*)*\\>')
            else: template = re.compile(like_to_regex(template) + '\\>')
            item = look.get(key, set())
            if item:
                item.add(template)
            else:
                item.add(template)
                look[key] = item
        return look

    def __find(self, look, start, end):
        """Searches for the next tag fitting the tag definition of an already converted like parameter (see __look)
        beginning from the current document position. It returns the position of the found tag and the tag.
        If not found, it returns the unaltered position and an empty string."""
        tag = ""
        next = start
        for search, rex_list in look.items():
            pos = self.pymala.find(search, start, end)
            while pos >= 0:
                gt = self.pymala.find('>', pos, end)
                if gt < 0: break  # no valid tag possible
                gt += 1
                tag = self.pymala[pos:gt]
                for rex in rex_list:
                    if rex.match(tag):
                        end = pos
                        tag = tag
                        next = gt
                        break
                if gt > end: break
                pos = self.pymala.find(search, gt, end)
        return (next, tag)

    def __extract(self, tag, start, end):
        """Looks for the corresponding end tag of the current tag.
        The start position should be directly after the specified tag."""
        if tag.endswith('/>') or tag.endswith('?>') or tag.startswith('<?'): return start
        op_tag = tag[1:-1].lstrip().partition(' ')[0] # remove tags '<>' and pick the first word
        if op_tag.startswith('/'): cl_tag = op_tag.lstrip('/')
        else: cl_tag = '/'+op_tag
        op = self.__look(op_tag)
        cl = self.__look(cl_tag)
        ballance = 1
        pos, tag = self.__find(cl, start, end)
        while tag:
            start, tag = self.__find(op, start, pos)
            while tag:
                ballance += 1
                start, tag = self.__find(op, start, pos)
            ballance -= 1
            if ballance == 0: return pos
            pos, tag = self.__find(cl, pos, end)
        return end

    def __next(self, start):
        """Returns the next tag while progressing through the document. If there are no tags left, it returns 
        an empty string."""
        pos = self.pymala.find('<', start, self.end)
        if pos < 0: return ("", start)
        gt = self.pymala.find('>', pos, self.end)
        if gt < 0: return ("", start)
        gt += 1
        return (self.pymala[pos:gt], gt)

    def __content(self, start):
        """Returns a the next content following the last tag until another tag is encountered."""
        pos = self.pymala.find('<', start)
        if pos < 0: pos = len(self.pymala)
        return (self.pymala[start:pos], pos)

class Timer:
    def __init__(self):
        self.start = 0
        self.elapsed = 0
    
    def go(self):
        self.start = time()

    def stop(self):
        self.elapsed += time() - self.start

    def reset(self):
        self.elapsed = 0

def like_to_regex(like):
    """Transforms a like-string with '?' (any char) and '*' (any number of chars) placeholders into a 
    regular expression string."""
    return ''.join(list(map(lambda x: x.replace('*','.*').replace('?','.') if x in "?* " or ((x.isidentifier() or x.isdigit()) and x.isascii()) else '\\'+x, [c for c in like])))

def parse_argv(argv, flags, para = None):
    """Parse the argv list. The flags list contains tupel with the flag, e.g. "-para", and the number of
    parameters. If this value is zero the parameter is a switch.
    You can define alternative flag names by separating them with a "|" character. Only the first one will
    be used as reference. As a flag always has a minus prefix, you can omitt it in the definition.
    The returned tupel contains the remaining arguments not used and a dictionary of the parameters."""
    if para == None: para = {}
    for p, d in flags:
        p = [v.strip().lstrip('-') for v in p.split('|')]
        f = ['-'+v for v in p]
        p = p[0]
        l = [j for i in range(len(argv)-d) if argv[i] in f for j in range(i, i+d+1)]
        if l:
            if d == 0: para[p] = 'True'
            elif d == 1: para[p] = argv[l[-1]]
            else: para[p] = [argv[i] for i in l[1:]]
            argv = [argv[i] for i in range(len(argv)) if not i in l]
    return (argv, para)

def parse_line(line, flags, para):
    """Parses the line if it contains one of the flags (see parse_argv). A line parameter is
    always followed by a colon and a value for the parameter. A potential minus prefix will be
    ignored. You can use the same flag list as for the parse_argv function, including alternative
    flags and default values. If a parameter is already defined, it will not be overwritten."""
    for p, d in flags:
        p = [v.strip().lstrip('-') for v in p.split('|')]
        f = '|'.join([v for v in p])
        p = p[0]
        if re.match(f"({f})\\s*:.*", line):
            if not p in para or not para[p]:
                line = line.partition(':')[2].strip().strip('"').strip("'")
                if d == 0: para[p] = "True" if line.lower() == "true" else "False"
                elif d == 1: para[p] = line
                else: para[p] = [v.strip().strip('"').strip("'") for v in line.split(',')]
            return True  
    return False

def mp_read_collect(reader, pymala_path, out):
    p = reader.next()
    while not p == None:
        lines = pymala_path.collect(p)
        out.put(lines)
        p = reader.next()
    out.put(None)

def main(argv):
    if len(argv) <= 1:
        print("PyMaLa - python markup-language to flat file converter")
        print("version 2024.02.08")
        print("pymala.py <script-file> [options ...]")
        print("options:")
        print("-input <input_template> : declares the document files using placeholders (* = any no of chars, ? = single char)")
        print("                          i.e.: -inp data*\\doc_*.xml")
        print('                          browse through directories starting with "data" selecting xml files starting with "doc_"')
        print("-inp <input_template>   : shortcut for -input")
        print("-output <output_file>   : target file for the tab-delimited data")
        print("-out <output_file>      : shortcut for -output")
        print("-root <root>            : root tag definintion identifying an entity (only required for multi-entity files)")
        print("-mp <processes>         : activates multiprocessing by assigning a number of processes to the task")
        print("                          if the no is negative or zero, it declares the CPUs not used for the task")
        print("                          file access may become a bottleneck for large numbers of assigned processes")
        print("-chunk <size>           : separates larger multi-entity files into chunks of <size> MB to enable multiprocessing")
        print("                          every chunk is considered a separate file to be distributed to a process")
        print("                          requires a distinct root definition and should not be applied for single-entity files")
        print('-encoding <enc>         : declares the encoding of the document files, e.g. latin1, ansi, utf-8 (default)')
        print('-info                   : concludes with some statistics (requires "true" or "false" as setting in the script)')
        print('                          docs = number of documents or chunks, pymala = number of pymala entities,')
        print('                          rows = number of lines in output, proc = number of processes,')
        print('                          clog = congestion of output process (it cannot keep pace with parsing if close to 100%)')
        print('                          time = run time for parsing without initialization')
        print("options override corresponding settings in the script file")
        print("a setting is not preceded by a minus and its parameter is separated by a colon, i.e. info: true")
        return
    argv = argv[1:]
    args = [('inp|input', 1), ('out|output', 1), ('root', 1), ('chunk', 1), ('mp', 1), ('rp', 1), ('info',0), ('encoding', 1)]
    (argv, para) = parse_argv(argv, args)
    if not argv: raise SyntaxError("no script file specified")
    script = path.realpath(argv[0])
    if not path.splitext(script)[1] and not path.isfile(script): script += ".mala"
    para["script"] = path.realpath(script)
    argv.pop(0)
    if argv: raise SyntaxError(f"invalid parameter: {' '.join(argv)}")
    header = PymalaTable()
    pymala = None
    option = 'inp','out' in para
    with open(para['script'], "rb") as script:
        line = script.readline().decode()
        while line:
            line = line.strip()
            if not line or line.startswith('#'):
                pass
            elif parse_line(line, args, para):
                pass
            elif re.match("header\\s*:.*", line):
                if pymala: raise SyntaxError("headers have to be declared before pymalas")
                header.append(line.partition(':')[2])
            else: 
                if not pymala: pymala = PymalaPath(header)
                pymala.add(line)
            line = script.readline().decode()
    cwd = getcwd()
    chdir(path.split(para["script"])[0])  # adjusting paths to PyMaLa script
    if "inp" in para: para["inp"] = path.realpath(para["inp"])
    if "out" in para: para["out"] = path.realpath(para["out"])
    chdir(cwd)
    mp = min(int(para.get("mp", '1')), cpu_count())
    if mp <= 0: mp = cpu_count() + mp
    if mp < 1: mp = 1
    if pymala.missing(): raise SyntaxError(f"undefined header field: {', '.join(pymala.missing())}")
    reader = PymalaReader(para['inp'], root = para.get('root'), chunk = int(para.get('chunk', 0)), encoding = para.get('encoding', 'utf-8'))
    output = open(para.get('out'), mode = 'w') if not para.get('out') in (None, 'stdout') else sys.stdout
    output.write(pymala.header()+'\n')
    docs = reader.size()
    mp = min(docs, mp)
    qsize = mp * 4
    pymalas = 0 
    jam = 0
    rows = 0
    t = Timer()
    t.go()
    if mp <= 1:
        call = 1
        jam = 0
        p = reader.next()
        while p:
            pymalas += 1
            for line in pymala.collect(p): 
                if line:
                    rows += 1
                    output.write(line+'\n')
            p = reader.next()
    else:
        running = mp
        out = Queue(qsize)
        for i in range(mp):
            Process(target = mp_read_collect, args = (reader, pymala, out)).start()
        while True:
            lines = out.get()
            if lines == None:
                running -= 1
                if not running: break
            else:
                jam += out.qsize()
                pymalas += 1
                for line in lines:
                    if line:
                        rows += 1
                        output.write(line+'\n')
    t.stop()
    if output != sys.stdout: 
        output.flush()
        output.close()
    if 'info' in para: print(f"docs {docs}\npyml {pymalas}\nrows {rows}\nproc {mp}\nclog {round(jam/pymalas/qsize*100,3)}%\ntime {round(t.elapsed,3)}s")
   
if __name__ == "__main__": sys.exit(main(sys.argv))
