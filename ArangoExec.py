import sublime, sublime_plugin, http.client, socket, types, threading, time, json, os, re

class Options:
    def __init__(self, name):
        self.name     = name
        connections   = sublime.load_settings("ArangoExec.sublime-settings").get('connections')
        conn = connections[self.name]
        self.host             = conn['host']
        self.port             = conn['port']
        self.username         = conn['username']
        self.password         = conn['password']
        self.database         = conn['database']
        self.service          = conn['service'] if 'service' in conn else None
        self.resultFileName   = conn['resultFileName'] if 'resultFileName' in conn else ''
        self.autoSave         = conn['autoSave'] if 'autoSave' in conn else False
        self.resultCount      = conn['resultCount'] if 'resultCount' in conn else True
        self.batchSize        = conn['batchSize'] if 'batchSize' in conn else 1000

    def __str__(self):
        return self.name

    @staticmethod
    def list():
        names = []
        connections = sublime.load_settings("ArangoExec.sublime-settings").get('connections')
        for connection in connections:
            names.append(connection)
        names.sort()
        return names

class Command():
    def __init__(self):
      self._resultView = None

    FILE_TYPE_HTML = "html"
    FILE_TYPE_JSON = "json"
    FILE_TYPE_XML = "xml"
    MAX_BYTES_BUFFER_SIZE = 8192
    HTML_CHARSET_HEADER = "CHARSET"
    htmlCharset = "utf-8"

    def explain(self, query, clear):
        bindVars = self._bindVarsFromComments(query)
        if bindVars:
          requestObject = { 'query' : query, 'bindVars': bindVars }
        else:
          requestObject = { 'query' : query }
        urlPart = "/_api/explain"
        respBodyText = self._execute(requestObject, urlPart)
        self._showResult(respBodyText, clear)

    def execute(self, query, clear):
        options = self._getOptions()
        bindVars = self._bindVarsFromComments(query)
        if bindVars:
          requestObject = { 'query' : query, 'bindVars': bindVars, 'count' : options.resultCount, 'batchSize': options.batchSize }
        else:
          requestObject = { 'query' : query, 'count' : options.resultCount, 'batchSize': options.batchSize }
        urlPart = "/_api/cursor"
        respBodyText = self._execute(requestObject, urlPart)
        self._showResult(respBodyText, clear)

    def fillDatabaseCollections(self):
        global collections

        query = "for e in Collections() filter SUBSTRING(e.name, 0, 1) != '_' return e.name"
        requestObject = { 'query' : query, 'count' : True, 'batchSize' :100 }
        urlPart = "/_api/cursor"
        respBodyText = self._execute(requestObject, urlPart)
        obj = json.loads(respBodyText)

        collections = []
        for collectionName in obj['result']:
            collections.append((collectionName, collectionName))

    def _bindVarsFromComments(self, queryText):
      result = re.search(r'^.*bindVars\s*:\s*(\{.*\})\s*$', queryText, re.M)
      if result:
        try:
          return json.loads(result.group(1))
        except:
          return
    
    def _makeFileName(self, viewFileName, resultFileName):
      if not viewFileName or not resultFileName:
        return None

      directory = os.path.dirname(viewFileName)
      basename = os.path.basename(viewFileName)
      if '*' in resultFileName:
        return os.path.join(directory, resultFileName.replace('*', os.path.splitext(basename)[0]))
      else:
        return os.path.join(directory, resultFileName)
    
    def _showToResultFile(self, filename, prettyRespBodyText, clear):
        if not self._resultView:
          if not os.path.exists(filename):
            #print('creating: ' + filename)
            directory = os.path.dirname(filename)
            if not os.path.exists(directory):
              os.makedirs(directory)
            open(filename, 'a').close()
          
          #print('opening(1): ' + filename)
          self._resultView = sublime.active_window().open_file(filename)
        
        if self._resultView.is_loading():
          #print('loading: ' + filename)
          sublime.set_timeout(lambda: self._showToResultFile(filename, prettyRespBodyText, clear), 100)
        else:
          #print('opening(2): ' + filename)
          self._resultView = sublime.active_window().open_file(filename)
          options = self._getOptions()
          
          if clear:
            self._resultView.run_command('select_all')
            self._resultView.run_command('left_delete')
          self._resultView.run_command('append', {'characters': prettyRespBodyText})
          if options.autoSave:
            self._resultView.run_command('save')
          
          # TODO: this really needs to be a context
          self._resultView = None

    def _showToConsole(self, prettyRespBodyText, clear):
        panel = sublime.active_window().get_output_panel("arango_panel_output")
        panel.set_read_only(False)
        panel.set_syntax_file("Packages/JavaScript/JSON.tmLanguage")
        if clear:
          panel.run_command('select_all')
          panel.run_command('left_delete')
        panel.run_command('append', {'characters': prettyRespBodyText})
        panel.set_read_only(True)
        sublime.active_window().run_command("show_panel", {"panel": "output.arango_panel_output"})

    def _showResult(self, respBodyText, clear):
        options = self._getOptions()
        
        obj = json.loads(respBodyText)
        prettyRespBodyText = json.dumps(obj,
                                  indent = 2,
                                  ensure_ascii = False,
                                  sort_keys = True,
                                  separators = (',', ': ')) + '\n\n'
        if not options.resultFileName:
          self._showToConsole(prettyRespBodyText, clear)
        else:
          active_view = sublime.active_window().active_view()
          if not active_view:
            return
          filename = self._makeFileName(active_view.file_name(), options.resultFileName)
          if not filename:
            print('cannot create filename')
            return
          self._showToResultFile(filename, prettyRespBodyText, clear)
          
    def _getOptions(self):
        global selectedIndexOptions

        if selectedIndexOptions == -1 :
            selectedIndexOptions = 0

        names = Options.list()
        options = Options(names[selectedIndexOptions])
        return options
    
    def _execute(self, requestObject, urlPart):
        options = self._getOptions()
        host = options.host
        port = options.port
        timeoutValue = 500
        request_page = "/_db/"+ options.database + urlPart
        requestPOSTBody = json.dumps(requestObject)
        requestType = "POST"

        try:
            # if not(useProxy):
                #if httpProtocol == self.HTTP_URL:
            conn = http.client.HTTPConnection(host, port, timeout=timeoutValue)
                # else:
                #     if len(clientSSLCertificateFile) > 0 or len(clientSSLKeyFile) > 0:
                #         print "Using client SSL certificate: ", clientSSLCertificateFile
                #         print "Using client SSL key file: ", clientSSLKeyFile
                #         conn = httplib.HTTPSConnection(
                #             url, port, timeout=timeoutValue, cert_file=clientSSLCertificateFile, key_file=clientSSLKeyFile)
                #     else:
                #         conn = httplib.HTTPSConnection(url, port, timeout=timeoutValue)

            conn.request(requestType, request_page, requestPOSTBody)

            # else:
            #     print "Using proxy: ", proxyURL + ":" + str(proxyPort)
            #     conn = httplib.HTTPConnection(proxyURL, proxyPort, timeout=timeoutValue)
            #     conn.request(requestType, httpProtocol + url + request_page, requestPOSTBody)

            startReqTime = time.time()
            resp = conn.getresponse()
            endReqTime = time.time()

            startDownloadTime = time.time()
            (respHeaderText, respBodyText, fileType) = self.getParsedResponse(resp)
            endDownloadTime = time.time()

            latencyTimeMilisec = int((endReqTime - startReqTime) * 1000)
            downloadTimeMilisec = int((endDownloadTime - startDownloadTime) * 1000)

            #respText = self.getResponseTextForPresentation(respHeaderText, respBodyText, latencyTimeMilisec, downloadTimeMilisec)
            
            conn.close()

            return respBodyText

        except (socket.error, http.client.HTTPException, socket.timeout) as e:
            print(e)
        except AttributeError as e:
            print(e)
            respText = "HTTPS not supported by your Python version"

    def getParsedResponse(self, resp):
        fileType = self.FILE_TYPE_HTML
        resp_status = "%d " % resp.status + resp.reason + "\n"
        respHeaderText = resp_status

        for header in resp.getheaders():
            respHeaderText += header[0] + ":" + header[1] + "\n"

            # get resp. file type (html, json and xml supported). fallback to html
            if header[0] == "content-type":
                fileType = self.getFileTypeFromContentType(header[1])

        respBodyText = ""
        self.contentLenght = int(resp.getheader("content-length", 0))

        # download a 8KB buffer at a time
        respBody = resp.read(self.MAX_BYTES_BUFFER_SIZE)
        numDownloaded = len(respBody)
        self.totalBytesDownloaded = numDownloaded
        while numDownloaded == self.MAX_BYTES_BUFFER_SIZE:
            data = resp.read(self.MAX_BYTES_BUFFER_SIZE)
            respBody += data
            numDownloaded = len(data)
            self.totalBytesDownloaded += numDownloaded

        respBodyText += respBody.decode(self.htmlCharset, "replace")

        return (respHeaderText, respBodyText, fileType)

    def getFileTypeFromContentType(self, contentType):
        fileType = self.FILE_TYPE_HTML
        contentType = contentType.lower()

        print ("File type: ", contentType)

        for cType in self.httpContentTypes:
            if cType in contentType:
                fileType = cType

        return fileType

    def getResponseTextForPresentation(self, respHeaderText, respBodyText, latencyTimeMilisec, downloadTimeMilisec):
        return respHeaderText + "\n" + "Latency: " + str(latencyTimeMilisec) + "ms" + "\n" + "Download time:" + str(downloadTimeMilisec) + "ms" + "\n\n\n" + respBodyText

def arangoChangeConnection(index):
    global selectedIndexOptions, command
    names = Options.list()
    selectedIndexOptions = index
    sublime.status_message(' ArangoExec: switched to %s' % names[index])
    command.fillDatabaseCollections()


class arangoListConnection(sublime_plugin.TextCommand):
    def run(self, edit):
        sublime.active_window().show_quick_panel(Options.list(), arangoChangeConnection)

class ArangoExplainCommand(sublime_plugin.TextCommand):

    def run(self, edit):
        global command
        Options.list()
        clear = True
        for region in self.view.sel():
            # If no selection, use the entire file as the selection
            query = ''
            if region.empty() :
                query = self.view.substr(sublime.Region(0, self.view.size()))
            else:
                query = self.view.substr(sublime.Region(region.a, region.b))

            command.explain(query, clear)
            clear = False

class ArangoExecCommand(sublime_plugin.TextCommand):

    def run(self, edit):
        global command
        Options.list()
        clear = True
        for region in self.view.sel():
            # If no selection, use the entire file as the selection
            query = ''
            if region.empty() :
                query = self.view.substr(sublime.Region(0, self.view.size()))
            else:
                query = self.view.substr(sublime.Region(region.a, region.b))

            command.execute(query, clear)
            clear = False

class ArangoExecAppendCommand(sublime_plugin.TextCommand):

    def run(self, edit):
        global command
        Options.list()
        for region in self.view.sel():
            # If no selection, use the entire file as the selection
            query = ''
            if region.empty() :
                query = self.view.substr(sublime.Region(0, self.view.size()))
            else:
                query = self.view.substr(sublime.Region(region.a, region.b))

            command.execute(query, False)

class ArangoAutoComplete(sublime_plugin.EventListener):
    def on_query_completions(self, view, prefix, locations):
        global collections
        syntax = view.settings().get('syntax')
        if not syntax == "Packages/ArangoExec/Aql.tmLanguage":
            return []

        return collections

selectedIndexOptions = -1
collections = []
command = Command()