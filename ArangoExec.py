import sublime, sublime_plugin, http.client, socket, types, threading, time, os, re
import sys, decimal
try:
    # python 3 / Sublime Text 3
    from . import simplejson as json
    from .simplejson import OrderedDict
except ValueError:
    # python 2 / Sublime Text 2
    import simplejson as json
    from simplejson import OrderedDict

class Options:
    def __init__(self, name):
        self.name     = name
        connections   = sublime.load_settings("ArangoExec.sublime-settings").get('connections')
        conn = connections[self.name]
        self.host                 = conn['host']
        self.port                 = conn['port']
        self.username             = conn['username']
        self.password             = conn['password']
        self.database             = conn['database']
        # self.service              = conn['service'] if 'service' in conn else None
        self.resultFileName       = conn['resultFileName'] if 'resultFileName' in conn else None
        self.queryBatchSeparator  = conn['queryBatchSeparator'] if 'queryBatchSeparator' in conn else '//!'
        self.autoSave             = conn['autoSave'] if 'autoSave' in conn else False
        self.resultCount          = conn['resultCount'] if 'resultCount' in conn else True
        self.batchSize            = conn['batchSize'] if 'batchSize' in conn else 1000

    def __str__(self):
        return self.name

    @staticmethod
    def list():
      global selectedOptionName
      names = []
      settings = sublime.load_settings("ArangoExec.sublime-settings")
      default = settings.get('default');
      connections = settings.get('connections')
      # build names
      for connection in connections:
          names.append(connection)
      # set default
      if selectedOptionName == '':
        if default in names:
          selectedOptionName = default
        else:
          selectedOptionName = names[0]
      # return list
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

    @staticmethod
    def json_loads(text):
        return json.loads(text,
                          object_pairs_hook=OrderedDict,
                          parse_float=decimal.Decimal)

    @staticmethod
    def json_dumps(obj):
        return json.dumps(obj,
                          indent=2,
                          ensure_ascii=False,
                          sort_keys=False,
                          separators=(',', ': '),
                          use_decimal=True)

    def explain(self, view, queryText, clear):
        options = self._getOptions()
        queries = self._queryBatches(queryText)
        _clear = clear
        for query in queries:
          bindVars = self._bindVarsFromComments(query)
          if bindVars:
            requestObject = { 'query' : query, 'bindVars': bindVars }
          else:
            requestObject = { 'query' : query }
          urlPart = "/_api/explain"
          respBodyText = self._execute(requestObject, urlPart)
          self._showResult(view, respBodyText, _clear)
          _clear = False

    def execute(self, view, queryText, clear):
        options = self._getOptions()
        queries = self._queryBatches(queryText)
        _clear = clear
        for query in queries:
          bindVars = self._bindVarsFromComments(query)
          if bindVars:
            requestObject = { 'query' : query, 'bindVars': bindVars, 'count' : options.resultCount, 'batchSize': options.batchSize }
          else:
            requestObject = { 'query' : query, 'count' : options.resultCount, 'batchSize': options.batchSize }
          urlPart = "/_api/cursor"
          respBodyText = self._execute(requestObject, urlPart)
          self._showResult(view, respBodyText, _clear)
          _clear = False

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

    def _queryBatches(self, queryText):
      options = self._getOptions()
      if not options.queryBatchSeparator:
        queries = [append(queryText)]
      else:
        queries = [x for x in re.split(options.queryBatchSeparator, queryText) if x.strip()]
        # queries = [x for x in queryText.split(options.queryBatchSeparator) if x.strip()]
      return queries

    def _bindVarsFromComments(self, queryText):
      result = re.search(r'^\s*bindVars\s*:\s*(\{.*\})\s*$', queryText, re.M)
      if result:
        try:
          return json.loads(result.group(1))
        except:
          print('Error parsing bindVars: ' + result.group(0)) 
          return
    
    def _makeFileName(self, viewFileName, resultFileName):
      if not viewFileName or not resultFileName:
        return None

      directory = os.path.dirname(viewFileName)
      basename = os.path.basename(viewFileName)
      splitbasename = os.path.splitext(basename)
      
      if splitbasename[1] and not splitbasename[1] == '.aql':
        print('File ext not aql: ' + basename)
        return None
      
      if '*' in resultFileName:
        return os.path.join(directory, resultFileName.replace('*', splitbasename[0]))
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

    def _showResult(self, view, respBodyText, clear):
        options = self._getOptions()
        
        #obj = json.loads(respBodyText)
        #
        #prettyRespBodyText = json.dumps(obj,
        #                          indent = 2,
        #                          ensure_ascii = False,
        #                          #sort_keys = False,
        #                          separators = (',', ': ')) + '\n\n'
        
        try:
          obj = self.json_loads(respBodyText)
          prettyRespBodyText = self.json_dumps(obj) + '\n\n'
        except Exception:
          prettyRespBodyText = respBodyText
          #prettyRespBodyText = str(sys.exc_info()[1]) #respBodyText
        
        if not options.resultFileName:
          self._showToConsole(prettyRespBodyText, clear)
        else:
          if not view:
            return
          filename = self._makeFileName(view.file_name(), options.resultFileName)
          if not filename:
            return
          self._showToResultFile(filename, prettyRespBodyText, clear)
          
    def _getOptions(self):
        options = Options(selectedOptionName)
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

            print(self.getResponseTextForPresentation(respHeaderText, latencyTimeMilisec, downloadTimeMilisec))
            
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

    def getResponseTextForPresentation(self, respHeaderText, latencyTimeMilisec, downloadTimeMilisec):
        return "\nArangoExec...\n" + respHeaderText + "\n" + "Latency: " + str(latencyTimeMilisec) + "ms" + "\n" + "Download time:" + str(downloadTimeMilisec) + "ms"

def arangoChangeConnection(index):
    global selectedOptionName, command
    if index != -1:
      names = Options.list()
      selectedOptionName = names[index]
      sublime.load_settings("ArangoExec.sublime-settings").set('default', selectedOptionName)
      sublime.save_settings("ArangoExec.sublime-settings")
      sublime.status_message(' ArangoExec: switched to %s' % selectedOptionName)
      command.fillDatabaseCollections()


class arangoListConnection(sublime_plugin.TextCommand):
    def run(self, edit):
      names = Options.list()
      if selectedOptionName in names:
        selected_idx = names.index(selectedOptionName)
      else:
        selected_idx = -1
      sublime.active_window().show_quick_panel(names, arangoChangeConnection, selected_index=selected_idx)

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

            command.explain(self.view, query, clear)
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

            command.execute(self.view, query, clear)
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

            command.execute(self.view, query, False)

class ArangoAutoComplete(sublime_plugin.EventListener):
    def on_query_completions(self, view, prefix, locations):
        global collections
        syntax = view.settings().get('syntax')
        if not syntax == "Packages/ArangoExec/Aql.tmLanguage":
            return []

        return collections

selectedOptionName = ''
collections = []
command = Command()