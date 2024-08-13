#' @include Table.R Variable.R api_request.R
Upload <- setRefClass("Upload",
   fields = list(name="character", table="Table", properties="list"),
   methods = list(
     show = function(){
       print(str_interp("<Upload ${.self$table$qualified_reference}#${.self$name}>"))
     },
    get = function(){
      .self$properties = make_request(path=get_upload_uri(.self))
    },
    exists = function(){
      res <- make_request(method="HEAD", path=.self$uri, stop_on_error=FALSE)
      if (length(res$error)){
        if (res$status == 404){
          return(FALSE)
        } else {
          stop(res$error)
        }
      } else {
        return(TRUE)
      }
    },
    delete = function(){
      make_request(method="DELETE", path=get_upload_uri(.self))
    },
    list_variables = function(max_results){
      # TODO: encodeURIComponent(.self$name)
      variables <- make_paginated_request(
        path=str_interp("${get_upload_uri(.self)}/variables"),
        page_size=100,
        max_results=max_results
      )
      purrr::map(variables, function(variable_properties) {
        Variable$new(name=variable_properties$name, table=.self, properties=variable_properties)
      })
    },
    create = function(
      data=NULL,
      type=NULL,
      transfer_specification=NULL,
      delimiter=NULL,
      schema=NULL,
      metadata=NULL,
      has_header_row=FALSE,
      skip_bad_records=FALSE,
      has_quoted_newlines=NULL,
      quote_character=NULL,
      rename_on_conflict=FALSE,
      replace_on_conflict=FALSE,
      allow_jagged_rows=FALSE,
      if_not_exists=FALSE,
      remove_on_fail=FALSE,
      wait_for_finish=TRUE,
      raise_on_fail=TRUE
    ) {
      # resumable_upload_id = None
      #
      # if data and (
      #   (hasattr(data, "read") and os.stat(data.name).st_size > 1e7)
      #   or (hasattr(data, "__len__") and len(data) > 1e7)
      # ):
      #   resumable_upload_id = upload_file(data, self.table.uri)
      #   data = None
      #   elif data and not hasattr(data, "read"):
      #     data = io.StringIO(data)
      #
      #   if schema and type != "stream":
      #     warnings.warn("The schema option is ignored for uploads that aren't of type `stream`")
      #
      #   exists = self.exists()
      #
      #   if if_not_exists and exists:
      #     return self
      #
      #   if replace_on_conflict is True and rename_on_conflict is True:
      #     raise Exception("Invalid parameters. replace_on_conflict and rename_on_conflict cannot both be True.")
      #
      #   if exists:
      #     if replace_on_conflict is True:
      #     self.delete()
      #   elif rename_on_conflict is not True:
      #     raise Exception(f"An upload with the name {self.name} already exists on this version of the table. If you want to upload this file anyway, set the parameter rename_on_conflict=True or replace_on_conflict=True.")
      #
      #   files = None
      #   payload = {
      #     "name": self.name,
      #     "type": type,
      #     "schema": schema,
      #     "metadata": metadata,
      #     "hasHeaderRow": has_header_row,
      #     "skipBadRecords": skip_bad_records,
      #     "hasQuotedNewlines": has_quoted_newlines,
      #     "allowJaggedRows": allow_jagged_rows,
      #     "quoteCharacter": quote_character,
      #     "delimiter": delimiter,
      #     "resumableUploadId": resumable_upload_id,
      #     "transferSpecification": transfer_specification
      #   }
      #
      #   if data is not None:
      #     files={'metadata': json.dumps(payload), 'data': data}
      #   payload=None
      #
      #   response = make_request(
      #     method="POST",
      #     path=f"{self.table.uri}/uploads",
      #     parse_payload=data is None,
      #     payload=payload,
      #     files=files
      #   )
      #   self.properties = response
      #   self.uri = self.properties["uri"]
      #
      #   try:
      #     if (data or resumable_upload_id or transfer_specification) and wait_for_finish:
      #     while True:
      #     time.sleep(2)
      #   self.get()
      #   if self["status"] == "completed" or self["status"] == "failed":
      #     if self["status"] == "failed" and raise_on_fail:
      #     raise Exception(self["errorMessage"])
      #   break
      #   else:
      #     logging.debug("Upload is still in progress...")
      #   except Exception as e:
      #     if remove_on_fail:
      #     self.delete()
      #   raise Exception(str(e))
      #
      #   return self
    }
  )
)

get_upload_uri = function(upload){
  str_interp("${project$user$name}.${project$name}")
}

MAX_CHUNK_SIZE = 2 ** 23
perform_file_upload = function(upload){
  start_byte = 0
  retry_count = 0
  chunk_size = MAX_CHUNK_SIZE
  did_reopen_file = FALSE
  # is_file = True if hasattr(data, "read") else False
  # file_size = os.stat(data.name).st_size if is_file else len(data)
  #
  # if (is_file and hasattr(data, 'mode') and 'b' not in data.mode):
  #   data = open(data.name, 'rb')
  # did_reopen_file = True
  #
  # res = make_request(
  #   path=f"{table_uri}/resumableUpload",
  #   method="POST",
  #   payload={"size": file_size},
  # )
  # resumable_uri = res["url"]
  # resumable_upload_id = res["id"]
  #
  # while start_byte < file_size:
  #   end_byte = min(start_byte + chunk_size - 1, file_size - 1)
  # if is_file:
  #   data.seek(start_byte)
  # chunk = data.read(end_byte - start_byte + 1)
  # else:
  #   chunk = data[start_byte : end_byte + 1]
  #
  # try:
  #   res = requests.put(
  #     url=resumable_uri,
  #     headers={
  #       "Content-Length": f"{end_byte - start_byte + 1}",
  #       "Content-Range": f"bytes {start_byte}-{end_byte}/{file_size}",
  #     },
  #     data=chunk,
  #   )
  # res.raise_for_status()
  #
  # start_byte += chunk_size
  # retry_count = 0
  # except Exception as e:
  #   if retry_count > 20:
  #   print("A network error occurred. Upload failed after too many retries.")
  # raise e
  #
  # retry_count += 1
  # time.sleep(retry_count)
  # print("A network error occurred. Retrying last chunk of resumable upload.")
  # start_byte = retry_partial_upload(
  #   file_size=file_size, resumable_uri=resumable_uri
  # )
  #
  # if did_reopen_file:
  #   data.close()
  #
  # return resumable_upload_id

}

retry_partial_file_upload = function(retry_count=0, file_size, resumable_uri){
  # logging.debug("Attempting to resume upload")
  #
  # try:
  #   res = requests.put(
  #     url=resumable_uri,
  #     headers={"Content-Length": "0", "Content-Range": f"bytes */{file_size}"},
  #   )
  #
  # if res.status_code == 404:
  #   return 0
  #
  # res.raise_for_status()
  #
  # if res.status_code == 200 or res.status_code == 201:
  #   return file_size
  # elif res.status_code == 308:
  #   range_header = res.headers["Range"]
  #
  # if range_header:
  #   match = re.match(r"bytes=0-(\d+)", range_header)
  # if match.group(0) and not math.isnan(int(match.group(1))):
  #   return int(match.group(1)) + 1
  # else:
  #   raise Exception("An unknown error occurred. Please try again.")
  # # If GCS hasn't received any bytes, the header will be missing
  # else:
  #   return 0
  # except Exception as e:
  #   if retry_count > 10:
  #   raise e
  #
  # time.sleep(retry_count / 10)
  # retry_partial_upload(
  #   retry_count=retry_count + 1,
  #   file_size=file_size,
  #   resumable_uri=resumable_uri,
  # )
}
