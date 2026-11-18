"""Owned local server and explicit UI actions for sequence lifecycle acceptance."""

import json
import hashlib
import tempfile
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import time
from urllib.request import Request, urlopen


class HictTestServer:
    def __init__(self, jar, data, log):
        self.jar = Path(jar).resolve()
        self.data = Path(data).resolve()
        self.log = Path(log).resolve()
        self.process = None
        self.stream = None
        self.artifact = None

    def __enter__(self):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        environment = {
            **os.environ,
            "DATA_DIR": str(self.data),
            "PROCESSED_DIR": str(self.log.parent / "processed"),
            "VXPORT": str(port),
            "HICT_BIND_HOST": "127.0.0.1",
            "SERVE_WEBUI": "false",
        }
        self.artifact = self._snapshot_artifact()
        try:
            self.stream = self.log.open("w")
            self.process = subprocess.Popen(
                ["java", "-Xmx2g", "-jar", str(self.artifact), "start-server", "--serve-webui=false"],
                cwd=self.log.parent,
                env=environment,
                stdout=self.stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError(f"Server exited during startup, see {self.log}")
                try:
                    self.http("/version")
                    return self
                except OSError:
                    time.sleep(0.1)
            raise TimeoutError(f"Server startup timed out, see {self.log}")
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def _snapshot_artifact(self):
        # Never execute a JAR that a later build can overwrite underneath the JVM.
        before = self.jar.stat()
        digest = hashlib.sha256()
        with tempfile.NamedTemporaryFile(prefix=".hict-test-server-", suffix=".jar",
                                         dir=self.log.parent, delete=False) as target:
            artifact = Path(target.name)
            try:
                with self.jar.open("rb") as source:
                    while block := source.read(1024 * 1024):
                        target.write(block)
                        digest.update(block)
                after = self.jar.stat()
                if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                    raise RuntimeError("Server artifact changed while copying; finish the build before launching acceptance")
                self.log.with_suffix(".artifact.sha256").write_text(digest.hexdigest()+"  "+self.jar.name+"\n")
            except BaseException:
                artifact.unlink(missing_ok=True)
                raise
        return artifact

    def http(self, path, payload=None):
        body = None if payload is None else json.dumps(payload).encode()
        request = Request(
            self.url + path, data=body, headers={"Content-Type": "application/json"}
        )
        with urlopen(request, timeout=60) as response:
            return response.read()

    def __exit__(self, *_):
        if self.process is not None and self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=10)
        if self.stream is not None:
            self.stream.close()
        if self.artifact is not None:
            self.artifact.unlink(missing_ok=True)


def install_download_picker(page):
    """Provide a writable browser save picker backed by observable downloads."""
    page.add_init_script('''(() => {
      window.showOpenFilePicker=undefined;
      window.showSaveFilePicker=async options=>{
        let content;
        return {name:options.suggestedName,
          getFile:async()=>new File([content],options.suggestedName,{type:content.type}),
          createWritable:async()=>{const chunks=[];return {write:async chunk=>{chunks.push(chunk);},close:async()=>{
            content=new Blob(chunks,{type:chunks[0]?.type||'text/plain'});
            const a=document.createElement('a'),url=URL.createObjectURL(content);
            a.href=url;a.download=options.suggestedName;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
          },abort:async()=>{}};}};
      };
    })();''')


class BrowserCheck:
    def __init__(self, page, output, *, writable_picker=True):
        # Normal map/sequence checks use a writable picker backed by browser downloads.
        # File-destination acceptance overrides it to exercise cancel/failure/fallback paths.
        if writable_picker:
            install_download_picker(page)
        self.page = page
        self.output = Path(output)
        self.rows = []
        self.errors = []
        page.add_init_script("""window.addEventListener('unhandledrejection', event => {
            console.error('Unhandled promise in browser acceptance check', event.reason);
        });""")
        page.on("pageerror", lambda error: self.errors.append(str(error)))
        page.on(
            "console",
            lambda message: (
                self.errors.append(message.text) if message.type == "error" else None
            ),
        )

    def menu(self, menu, action):
        self.page.get_by_role("link", name=menu, exact=True).click()
        target = self.page.get_by_role("link", name=action, exact=True)
        for role in ("button", "menuitemradio", "menuitemcheckbox"):
            if target.count():
                break
            target = self.page.get_by_role(role, name=action, exact=True)
        target.click()

    def advanced(self):
        for detail in self.page.locator('[role="dialog"]:visible details').all():
            if (
                detail.locator("summary").inner_text().strip() == "Advanced"
                and detail.get_attribute("open") is None
            ):
                detail.locator("summary").click()

    def choose_file(self, filename):
        dialog = self.page.locator(".modal.show").last
        table_cell = dialog.locator(".file-name-cell").get_by_text(filename, exact=True)
        if table_cell.count():
            table_cell.click()
        else:
            dialog.locator("span[title]").filter(
                has_text=re.compile("^" + re.escape(filename) + "$")
            ).click()

    def settle(self):
        self.page.locator(".open-progress-modal").wait_for(state="hidden")
        self.page.evaluate("""() => {
            const manager=window.__hictBenchmark.manager();
            if(manager){manager.getMap().on('rendercomplete',()=>{});manager.getMap().renderSync();}
        }""")
        self.page.wait_for_function("""() => {
            const manager=window.__hictBenchmark.manager();
            if(!manager)return true;
            const map=manager.getMap();
            if(!map.getView().getAnimating() && map.tileQueue_.getTilesLoading()===0 && map.tileQueue_.getCount()===0)map.renderSync();
            return !map.getView().getAnimating() && map.renderComplete_
                && map.tileQueue_.getTilesLoading()===0 && map.tileQueue_.getCount()===0;
        }""")
        self.page.wait_for_timeout(300)

    def select_range(self, region=True):
        self.page.evaluate("""() => {
            const manager=window.__hictBenchmark.manager();
            manager.eventManager.resetSelection();
            manager.viewAndLayersManager.fitViewToActiveExtent();
        }""")
        self.settle()
        points = self.page.evaluate("""() => {
            const manager=window.__hictBenchmark.manager(),layers=manager.viewAndLayersManager;
            const holder=manager.contigDimensionHolder,d=layers.currentViewState.resolutionDesciptor;
            const rect=manager.getMap().getViewport().getBoundingClientRect();
            return [.25,.65].map(fraction=>{
                const bp=holder.contigDescriptors[0].contigLengthBp*fraction;
                const px=holder.getFractionalPxOfBp(bp,d.bpResolution)*d.pixelResolution;
                const point=manager.getMap().getPixelFromCoordinate([px,-px]);
                return [rect.left+point[0],rect.top+point[1]];
            });
        }""")
        modifier = "Control" if region else "Shift"
        self.page.keyboard.down(modifier)
        self.page.mouse.move(*points[0])
        self.page.mouse.down()
        self.page.mouse.move(*points[1], steps=12)
        self.page.mouse.up()
        self.page.keyboard.up(modifier)
        self.page.wait_for_function(
            "window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.selectionBorders.leftBP!==undefined" if region else
            "window.__hictBenchmark.manager().viewAndLayersManager.currentViewState.selectedContigIds.length>0"
        )

    def reverse(self):
        self.select_range(region=False)
        with self.page.expect_response("**/reverse_selection_range") as response:
            self.page.get_by_role(
                "button", name="Reverse Selection", exact=True
            ).click()
        assert response.value.ok, response.value.text()

    def step(self, name, action):
        try:
            action()
            self.settle()
            assert not self.errors, self.errors
            self.page.screenshot(path=str(self.output / (name + ".png")))
            self.rows.append({"test": name, "status": "passed"})
            print("Passed", name, flush=True)
        except BaseException as failure:
            self.rows.append({"test": name, "status": "failed", "error": str(failure)})
            self.page.screenshot(path=str(self.output / (name + "-failed.png")))
            raise
