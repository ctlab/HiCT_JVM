# Examples and Jobs

Use **File → Open Examples…** to choose a dataset. Starting a download opens
**Jobs**, focused on that dataset. Cached examples and **Open result** also start
a verification job before opening. Valid cached files are reused after checksum
verification; changed or damaged files are fetched again. Quick-start downloads show that the example
will open when ready. Closing Jobs lets the transfer continue and cancels that
automatic opening; **Open result** remains available when it finishes. Opening a
different workspace also revokes a pending download's automatic opening.

The Jobs button counts active jobs. Known-size transfers show bytes and a
percentage. Unknown-size transfers use an indeterminate bar and say that the total
size is unknown. Reference preparation and final file verification have their own
messages. Optional-file failures stay visible beside the completed result; they
are separate from a failed required download.

**Cancel** targets the selected attempt. Partial files remain available for a
later attempt. **Open Examples to resume** brings the matching dataset's action
into view. The downloader resumes compatible partial files and verifies the result;
a changed provider response can require restarting the file. A failed catalog
request has a separate **Retry loading examples** action.

The server retains at most 50 example download attempts, including the current
one. This history lasts for the server session. Downloaded files and manifests
remain under `DATA_DIR/examples/<example-id>` after a restart; choose the example
again to open completed data or continue downloading. Dataset descriptions,
publications, provider information and terms remain on the example cards.

Other operations also appear in Jobs. Closing their configuration dialog lets
the work continue; **Open result** delivers a completed result in its appropriate
map context. For **File → Open FASTA only…**, choose the file and select **Create
and open**. Its result can also be opened from Jobs. **Assembly → Export to AGP**
is available without contact data and describes the assembled sequence coordinates.

Quick-start recipes that need matrix conversion hand progress to a named conversion
job. Closing Jobs lets conversion finish and revokes automatic opening. Its **Open
result** returns through the example recipe, verifies cached inputs, reuses the
completed conversion and links the recipe FASTA. Changing workspace also revokes
automatic opening. Failed conversions remain in Jobs with a short failure message, expandable
**Error details** and **Open Examples to retry**, focused on the matching card. Recipes with assembly layouts/tracks retain the opening wizard. A page
reload does not restore an automatic-opening intent; use Jobs or the example card.
Stopping the server cancels active work rather than merely hiding progress.

A catalog change invalidates the earlier ready marker. Reopening verifies the files
against the current declaration and refreshes the recorded recipe and attribution.
Older manifests are refreshed by the same workflow. Checksums supplied by the
provider verify the declared content; without one, the manifest records the fetched
bytes and can detect later changes, but cannot establish provider authenticity.
