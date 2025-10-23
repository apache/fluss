---
title: Release Manager Preparation
sidebar_position: 1
---

# Release Manager Preparation

It is important that the release manager is well prepared before initiating the release process. This page provides a checklist of tasks that should be completed in advance.

We recommend that the release manager be a committer or a PPMC member, as they typically have the necessary permissions and in-depth understanding of the project. However, any contributor familiar with the Fluss project and who has the required access rights may serve as the release manager.

Note: The following setup is a one-time configuration required for release preparation.

## Environment Setup

This release process is suggested to operate on MacOS or Linux systems, and the following tools are required:

- Java 8
- Apache Maven 3.8.6
- GnuPG 2.x
- Git
- SVN
- [Helm](https://helm.sh/docs/intro/install/) and [helm-gpg](https://github.com/technosophos/helm-gpg) plugin
- [Docker](https://docs.docker.com/get-started/get-docker/)

:::note
If you encounter issues installing `helm-gpg` using the command  
```bash
helm plugin install https://github.com/technosophos/helm-gpg
```  
you can install it manually by cloning the repository to a local directory and then running:  
```bash
helm plugin install /path/to/your/helm-gpg
```  

This approach bypasses potential network or Git submodule issues during plugin installation.
:::


## GPG Key

You need to have a GPG key to sign the release artifacts. Please be aware of the ASF-wide [release signing guidelines](https://www.apache.org/dev/release-signing.html). If you don’t have a GPG key associated with your Apache account, please create one according to the guidelines.

### Generate a GPG Key

Following is a quick setup guide for generating GPG key.

Install GPG:

```
sudo apt install gnupg2
```

Generate a GPG key. Please use your apache name and email for generate key.

```bash
$ gpg --full-gen-key
gpg (GnuPG) 2.2.20; Copyright (C) 2020 Free Software Foundation, Inc.
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.

Please select what kind of key you want:
   (1) RSA and RSA (default)
   (2) DSA and Elgamal
   (3) DSA (sign only)
   (4) RSA (sign only)
  (14) Existing key from card
Your selection? 1 # input 1
RSA keys may be between 1024 and 4096 bits long.
What keysize do you want? (2048) 4096 # input 4096
Requested keysize is 4096 bits
Please specify how long the key should be valid.
         0 = key does not expire
      <n>  = key expires in n days
      <n>w = key expires in n weeks
      <n>m = key expires in n months
      <n>y = key expires in n years
Key is valid for? (0) 0 # input 0
Key does not expire at all
Is this correct? (y/N) y # input y

GnuPG needs to construct a user ID to identify your key.

Real name: Jark Wu                        # input your name
Email address: jark@apache.org            # input your email
Comment: CODE SIGNING KEY                 # input some annotations, optional
You selected this USER-ID:
    "Jark Wu <jark@apache.org>"

Change (N)ame, (C)omment, (E)mail or (O)kay/(Q)uit? O # input O
We need to generate a lot of random bytes. It is a good idea to perform
some other action (type on the keyboard, move the mouse, utilize the
disks) during the prime generation; this gives the random number
generator a better chance to gain enough entropy.
We need to generate a lot of random bytes. It is a good idea to perform
some other action (type on the keyboard, move the mouse, utilize the
disks) during the prime generation; this gives the random number
generator a better chance to gain enough entropy.

# Input the security key
┌──────────────────────────────────────────────────────┐
│ Please enter this passphrase                         │
│                                                      │
│ Passphrase: _______________________________          │
│                                                      │
│       <OK>                              <Cancel>     │
└──────────────────────────────────────────────────────┘
# key generation will be done after your inputting the key with the following output
gpg: key 9D27AF4D799 marked as ultimately trusted
gpg: revocation certificate stored as '/Users/wuchong/.gnupg/openpgp-revocs.d/3ADAEEABE828406F6AB2338B832539D27AF4D799.rev'
public and secret key created and signed.

pub   rsa4096 2025-08-07 [SC]
      3ADAEEABE828406F6AB2338B832539D27AF4D799
uid                Jark Wu (CODE SIGNING KEY) <jark@apache.org>
sub   rsa4096 2025-08-07 [E]
```

### Upload GPG public Key to Apache KEYS

Determine your Apache GPG Key and Key ID, as follows:

```bash
gpg --list-keys
```

This will list your GPG keys. One of these should reflect your Apache account, for example:

```
--------------------------------------------------
pub   2048R/845E6689 2016-02-23
uid                  Nomen Nescio <anonymous@apache.org>
sub   2048R/BA4D50BE 2016-02-23
```

Here, the key ID is the 8-digit hex string in the pub line: 845E6689.

Now, add your Apache GPG key to the Flink’s KEYS file in the [release](https://dist.apache.org/repos/dist/release/incubator/fluss/KEYS) repository at [dist.apache.org](https://dist.apache.org). Follow the instructions listed at the top of these files. (Note: Only PPMC members have write access to the release repository. If you end up getting 403 errors ask on the mailing list for assistance.) PPMC member can refer following scripts to add your Apache GPG key to the KEYS in the release repository.

```
svn co https://dist.apache.org/repos/dist/release/incubator/fluss fluss-dist
cd fluss-dist
(gpg --list-sigs <YOUR_KEY_ID> && gpg --armor --export <YOUR_KEY_ID>) >> KEYS
svn ci -m "[fluss] Add <YOUR_NAME>'s public key"
```

To send the key to the Ubuntu key-server, Apache Nexus needs it to validate published artifacts:

```
gpg --keyserver hkps://keyserver.ubuntu.com --send-keys <YOUR KEY ID HERE>
```

Configure git to use this key when signing code by giving it your key ID, as follows:

```
git config --global user.signingkey 845E6689
```

You may drop the `--global` option if you’d prefer to use this key for the current repository only.

You may wish to start `gpg-agent` to unlock your GPG key only once using your passphrase. Otherwise, you may need to enter this passphrase hundreds of times. The setup for `gpg-agent` varies based on operating system, but may be something like this:

```
eval $(gpg-agent --daemon --no-grab --write-env-file $HOME/.gpg-agent-info)
export GPG_TTY=$(tty)
export GPG_AGENT_INFO
```

### Upload the GPG public key to your GitHub account (Optional)

Enter https://github.com/settings/keys to add your GPG key.
Please remember to bind the email address used in the GPG key to your GitHub account (https://github.com/settings/emails) if you find "unverified" after adding it.

## Access to Apache Nexus repository

Configure access to the [Apache Nexus repository](https://repository.apache.org/), which enables final deployment of releases to the Maven Central Repository.

1. You log in with your Apache account.
2. Confirm you have appropriate access by finding `org.apache.fluss` under `Staging Profiles`.
3. Navigate to your Profile (top right dropdown menu of the page).
4. Choose `User Token` from the dropdown, then click `Access User Token`. Copy a snippet of the Maven XML configuration block.
5. Insert this snippet twice into your global Maven `settings.xml` file, typically `${HOME}/.m2/settings.xml`. The end result should look like this, where `TOKEN_NAME` and `TOKEN_PASSWORD` are your secret tokens:

```xml
<settings>
   <servers>
     <server>
       <id>apache.releases.https</id>
       <username>TOKEN_NAME</username>
       <password>TOKEN_PASSWORD</password>
     </server>
     <server>
       <id>apache.snapshots.https</id>
       <username>TOKEN_NAME</username>
       <password>TOKEN_PASSWORD</password>
     </server>
   </servers>
</settings>
```

## GNU Tar Setup for Mac

Skip this step if you are not using a Mac. The default tar application on Mac does not support GNU archive format and defaults to Pax. This bloats the archive with unnecessary metadata that can result in additional files when decompressing (see 1.15.2-RC2 vote thread). Install gnu-tar and create a symbolic link to use in preference of the default tar program.

```bash
brew install gnu-tar
ln -s /opt/homebrew/bin/gtar /usr/local/bin/tar
# Make sure which directory "gtar" is installed to, at the time of writing it is "/opt/homebrew/bin/gtar"
which tar
```


## Further reading

It's recommended but not mandatory to read following documents before making a release to know more details about apache release:

- Release policy: https://www.apache.org/legal/release-policy.html
- Incubator release: http://incubator.apache.org/guides/releasemanagement.html
- TLP release: https://infra.apache.org/release-distribution
- Release sign: https://infra.apache.org/release-signing.html
- Release publish: https://infra.apache.org/release-publishing.html
- Release download pages: https://infra.apache.org/release-download-pages.html
- Publishing maven artifacts: https://infra.apache.org/publishing-maven-artifacts.html

