# fake_cmd

``fake_cmd`` provides a simulated remote command line based on file communication, in order to send commands from the client to the server for execution. It applies to situations where two computers cannot be connected through the network (which means ``ssh`` is impossible), but can be mounted onto the same hard disk.

## Get Started

### Installation

#### Requirements

``slime_core`` is a necessary dependency, and install it as follows (Note that the version should be 0.0.1):

```bash
pip install slime-core==0.0.1
```

``pexpect`` is a recommended dependency (if you run the server on a Unix platform):

```bash
pip install pexpect
```

#### Install fake_cmd

Install ``fake_cmd`` as follows:

```bash
pip install fake_cmd
```

### Start the Server and the Client

On the server computer which you want to run your code on, start the server as follows:

```Python
from fake_cmd.core.server import Server

if __name__ == "__main__":
    Server("/specify/a/hard/disk/path/here").run()
```

On the client, start the client as follows:

```Python
from fake_cmd.core.client import CLI

if __name__ == "__main__":
    CLI("/same/path/as/the/server").run()
```

Alternatively, after installation you can start the server and client via commands:

```bash
# Start the server with address as a positional argument
fake-cmd-server /specify/a/hard/disk/path/here --max_cmds 8

# Start the client with address as a positional argument
fake-cmd-client /same/path/as/the/server --id_prefix abc
```

## Usage

In ``fake_cmd``, there is only one Server instance running at each address, and one Server can create multiple Sessions which are responsible for communicating with the Clients (and there is a one-to-one correspondence between Sessions and Clients). ``fake_cmd`` supports inner commands and command options for advanced usage, and you can simply use them in the Client CLI.

### Inner Commands

- ``help``: Get the help document.
- ``exit``: Shutdown the client and disconnect session.
- ``sid``: Get the session id of the client.
- ``ls-session``: List all the alive sessions.
- ``ls-cmd``: List all the commands executing or queued.
- ``ls-back``: List the background command of the current session.
- ``ls-server-version``: Show the server version (for compatibility check).
- ``version_strict_on``: Set the version strict to True.
- ``version_strict_off``: Set the version strict to False.

### Command Options

The command options can set advanced configurations when running the given command.

#### Usage

Required Syntax:

```
[Command options (as follows)] -- [Your real command]
```

Example:

```
inter --exec /bin/bash -- python -i
```

In the above example, ``inter --exec /bin/bash`` is the command options, and ``python -i`` is your real command to run on the server, `` -- `` serves as a delimiter to separate between the command options and the real command.

#### Supported Command Options

- ``cmd``: Run the command with advanced options. Use ``cmd -h`` to get more help in the Client.
- ``inter``: Run the command in the interactive mode. Input is enabled. Use ``inter -h`` to get more help in the Client.
- ``pexpect``: Run the command in an advanced interactive mode using the additional package ``pexpect`` (Availability: Unix). Use ``pexpect -h`` to get more help.

#### Examples

In ``fake_cmd``, if you run a command by directly entering the real command (e.g., ``ls``) or using ``cmd`` options (e.g., ``cmd --exec /bin/bash -- ls``), then the input is disabled, and you CANNOT send any input to the command running on the Session:

```bash
[fake_cmd XXX]$ python -i
# If you try to enter ``import random`` here, then nothing will happen.
>>> import random
```

If you use ``inter`` or ``pexpect`` (on Unix) to run the command, then the interactive mode is activated, and ``fake_cmd`` will read any of the input content from the client and send it to the command running on the Session:

```bash
# This is OK.
[fake_cmd XXX]$ inter -- python -i
>>> import random
>>> random.random()
0.8675832272860072
>>> 

# This is also OK (on Unix).
[fake_cmd XXX]$ pexpect -- python
>>> import random
>>> random.random()
0.14142611546447192
>>> 
```

> NOTE: If you are starting a Server on a Unix platform, then we recommend you to activate the interactive mode using ``pexpect`` rather than ``inter``, because the former provides more comprehensive functionality.

In ``fake_cmd``, when you are running a command on the Session, the default behavior of ``Ctrl-C`` is sending different kill signals to the Session according to the number of times ``Ctrl-C`` is pressed (See [Kill the Running Command](#kill-the-running-command) for more details). However, in the interactive mode, you may only want to send keyboard interrupts to the command by pressing ``Ctrl-C`` rather than killing it (for example, if you run ``inter -- python -i``, you may press ``Ctrl-C`` to interrupt the Python code rather than killing the Python itself). To solve this, you can run the command using the ``--kill_disabled`` option:

```bash
[fake_cmd XXX]$ pexpect --kill_disabled -- python
# Wow, ``Ctrl-C`` can now interrupt the Python code rather than killing Python itself!
>>> import time
>>> time.sleep(114514)
^CTraceback (most recent call last):
  File "<stdin>", line 1, in <module>
KeyboardInterrupt
>>> time.sleep(1919810)
^CTraceback (most recent call last):
  File "<stdin>", line 1, in <module>
KeyboardInterrupt
```

> NOTE: If you run a command in the interactive mode using ``inter``, you should explicitly activate interactive mode of the command (e.g., using ``python -i`` rather than ``python``, using ``/bin/bash -i`` rather than ``/bin/bash``, etc.). While if you use ``pexpect``, just run the command as usual (e.g., ``python``, ``/bin/bash``, etc.).

> WARNING: Here is one of the reasons that we recommend you to use ``pexpect`` rather than ``inter`` on Unix:
>
> In our tests, if you run bash using ``inter --kill_disabled -- /bin/bash -i``, and you run another command in the bash, then you will be unable to kill the command using ``Ctrl-C``, maybe because the SIGINT signal (i.e., the keyboard interrupt) can not be passed to the command. While if you use ``pexpect --kill_disabled -- /bin/bash``, which starts a pseudo terminal to run the command, then ``Ctrl-C`` will be successfully sent to the command just like the user really presses ``Ctrl-C`` in the command line.
>
> The following example shows this problem:
>
> ```bash
> [fake_cmd XXX]$ inter --kill_disabled -- /bin/bash -i
> # Now enter the bash command line.
> # The keyboard interrupt won't kill the ``sleep 114514`` command, and you can only kill it in another client using ``kill -9 pid``.
> [xxx ~]$ sleep 114514
> Keyboard Interrupt (Interactive Input).
> Keyboard Interrupt (Interactive Input).
> Keyboard Interrupt (Interactive Input).
>
> # Recommended on Unix.
> [fake_cmd XXX]$ pexpect --kill_disabled -- /bin/bash
> # Now enter the bash command line.
> [xxx ~]$ sleep 1919810
> Keyboard Interrupt (Interactive Input).
> # Yeah, you successfully killed it.
> [xxx ~]$ 
> ```

> NOTE: ``inter`` may fail to send the input when the command requires entering a password (e.g., ``sudo``), while ``pexpect`` can achieve this (the reasons are discussed in [Going Deeper into the Command Options](#going-deeper-into-the-command-options-optional-reading)).
> 
> WARNING: The password you enter will not be hidden by ``fake_cmd``, because ``fake_cmd`` will never know when the command requires a password, and it treats all inputs as regular ones. Example:
>
> ```bash
> [fake_cmd XXX]$ pexpect -- sudo ls
> Password:Oh, my password will not be hidden here.
> xxx (The output of ``ls``)
> ```

#### Going Deeper into the Command Options (Optional Reading)

If you run a command by directly entering it in the ``fake_cmd`` or using ``cmd`` or ``inter`` options, the Session will start the command using ``Popen`` in the standard Python library ``subprocess``. It can only redirect the input and output streams of the command to a pipe or a specified file, so it does not simulate a command line in the true sense, and in some cases it may fail to control the behavior of the command (for example, it fails to send a password to the command when you are running ``sudo``).

Different from ``Popen``, ``pexpect`` starts a new command by creating a pseudo terminal using ``pty.fork``, which can be seen as a real command line by the running command, and it supports more features such as entering a password, sending ``Ctrl-C`` to the command line (just like a real user does), etc.

### Kill the Running Command

If you are directly entering a command in ``fake_cmd``, using ``cmd`` or ``--kill_disabled`` is not specified in the interactive 
mode, the number of times ``Ctrl+C`` is pressed represents different behaviors:

- (1): Send keyboard interrupt to the command (SIGINT signal).
- (2): Terminate the command (SIGTERM signal).
- (3): Kill the command (SIGKILL signal on Unix, and SIGTERM signal on Windows).
- (>=4): Do not wait until the command is terminated, and put the command to the background (and then you can use ``ls-back`` to check it, or manually use ``kill -9 pid`` to kill it).

If ``--kill_disabled`` is specified in the interactive mode, one ``Ctrl+C`` corresponds to one keyboard interrupt sent to the command, and you may need to manually exit the command according to different commands (e.g., use ``exit()`` in the Python interactive mode, and use ``exit`` in the /bin/bash, etc.).

### Danger Zone

- ``server_shutdown``: Shutdown the whole server. NOTE: The server can be shutdown by any of the Clients, without any authorization required. When shutdown is executed, the Server will terminate all the commands and destroy all the sessions. BE CAREFUL TO USE IT!!!

## Other Things to Note

### About the Command-Line State

In ``fake_cmd``, each command you enter is executed separately, so the command-line state cannot be preserved between the commands. Example:

```bash
[fake_cmd XXX]$ pwd
/a/b/c
[fake_cmd XXX]$ cd ../
[fake_cmd XXX]$ pwd
# NOTE: the output of ``pwd`` here is still ``/a/b/c``, because the previous ``cd`` command and the ``pwd`` command are executed separately.
/a/b/c
[fake_cmd XXX]$ cd ../; pwd
# The output of ``pwd`` becomes ``/a/b`` here, because the ``cd`` command and the ``pwd`` command are executed in one command. Due to this, you can put all the commands in one shell file, and run it using ``sh``, ``/bin/bash``, etc.
/a/b
```

While, if you run bash using ``pexpect`` (or ``inter``), then in bash the state can be preserved, because the bash is executed once by ``fake_cmd``, and then it keeps running in the interactive mode. Example:

```bash
[fake_cmd XXX]$ pexpect --kill_disabled -- /bin/bash
# Now enter the bash command line.
[xxx c]$ pwd
/a/b/c
[xxx c]$ cd ../
[xxx b]$ pwd
# The state can be preserved by bash.
/a/b
# You can even use conda here.
[xxx b]$ conda init bash
[xxx b]$ source ~/.bashrc
[xxx b]$ conda activate base
# Yeah, conda activated successfully!
(base) [xxx b]$ 
```

### One Display Bug that Cannot be Eliminated 

``fake_cmd`` only simulates a remote command line by displaying the remote output in the Client and sending the input from the Client to the Session, and it doesn't implement low-level command line interfaces as ``ssh`` does. Because of this, there will be messed-up display in the interactive mode (especially when the ``readline`` module is enabled), for ``fake_cmd`` doesn't exactly know what the input prompt is in the Session command and simply uses ``input()`` with an empty prompt. Example:

```bash
[fake_cmd XXX]$ pexpect --kill_disabled -- /bin/bash
# Now enter the bash command line.
[xxx ~]$ 
# If you input something in the bash command line and delete it using backspace, then the whole line will be deleted (including the display content ``[xxx ~]$ ``)
[xxx ~]$ enter something here 114514
# Then delete using backspace
█ (Oops, the whole line is deleted)

# Further, if you input something that is longer than the command line width, messed-up display also occurs. One way to solve this problem (not quite as elegantly) is to press ``Ctrl-D`` (raise an EOFError) to start a new line.
[xxx ~]$ EOF (Interactive Input).
█ (Now I can input something here on a new line and no messed-up display will occur, because ``input()`` is used with an empty prompt, which aligns perfectly with the empty new line)
```

## License

``fake_cmd`` uses an MIT license, as found in the [LICENSE](LICENSE) file.
