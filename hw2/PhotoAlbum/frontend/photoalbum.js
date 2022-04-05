$(document).ready(function () {
    console.log("Ready!");


    function callSearchAPI(query) {
        var sdk = apigClientFactory.newClient({
            apiKey: "TZaSVqffYu6UgtjDGHLfr7YA1GPyw2DPINEWxTh1"
        });
        return sdk.searchGet({ q: query }, {}, {});
    }

    function callUploadAPI(image, customLabels) {
        var filename = document.getElementById("image-file").value;
        console.log(filename);
        var additionalparams = {
            headers: {
                "Content-Type": "image/jpeg",
                "x-amz-meta-customLabels": customLabels,
            }
        }
        var params = {
            'object': document.getElementById("image-file").value,
            'folder':'cc-photos-bucket',
            'Content-Type': "image/jpeg",
            'Content-Encoding': 'base64',
            'x-amz-meta-customLabels': customLabels
        };
        console.log(additionalparams);

        var sdk = apigClientFactory.newClient({
            apiKey: "TZaSVqffYu6UgtjDGHLfr7YA1GPyw2DPINEWxTh1"
        });
        return sdk.uploadFolderObjectPut(params, image, additionalparams);
    }

    function search() {
        var search_query = $('.search-query').val()
        console.log("Searching for " + search_query);
        $('.search-results').html("");
        callSearchAPI(search_query).then((response) => {
            var image_list = response.data.results;
            console.log(image_list);
            
            var html = '<br>';
            if (image_list.length == 0) {
                html += '<h3>No images found for given tags</h3>'
            } else {
                var i;
                for (i = 0; i < image_list.length; i++) {
                    html += '<img src="' + image_list[i].url + '" witdth="250" height="250" class="thumbnail" /><br><br>'
                }
            }
            console.log(html);
            $(html).appendTo($('.search-results'));
        });
    }

    function upload() {
        var img = document.getElementById("imagebase64").value;
        var comma_index = img.indexOf(",");
        var custom1 = document.getElementById("customLabel1").value;
        var custom2 = document.getElementById("customLabel2").value;
        var custom3 = document.getElementById("customLabel3").value;
        console.log( "custom1="+custom1 )
        console.log( "custom2="+custom2 )
        console.log( "custom3="+custom3 )
        var customLabels = custom1 + "," + custom2 + "," + custom3
        console.log( "customLabels="+customLabels )
        var image_str = img.substring(comma_index+1);

        callUploadAPI(image_str, customLabels).then((response) => {
            console.log(response);
        });
        $('.upload-message').html("File Uploaded");
    }

    $('.search-button').click(function () {
        search();
    });

    $('.upload-button').click(function () {
        upload();
    });

    $(window).on('keydown', function (e) {
        if (e.which == 13) {
            search();
            return false;
        }
    })

    function readFile() {
  
        if (this.files && this.files[0]) {

            var FR = new FileReader();
            console.log(this.files[0]['name']);
            var filename = this.files[0]['name']
            document.getElementById("image-file").setAttribute("value", filename);
            FR.addEventListener("load", function(e) {
                document.getElementById("img").src = e.target.result;
                document.getElementById("imagebase64").setAttribute("value", e.target.result);
            }); 

            FR.readAsDataURL( this.files[0] );
        }
        
    }

    document.getElementById("image").addEventListener("change", readFile);

    $(function () {
        // check for support (webkit only)
        if (!('webkitSpeechRecognition' in window)) // todo error
            return;
    
        var input = document.getElementById('input');
        var record = document.getElementById('record');
    
        // setup recognition
        const talkMsg = 'Speaking';
        // seconds to wait for more input after last
        const patience = 5;
        var recognizing = false;
        var timeout;
        var oldPlaceholder = null;
        var recognition = new webkitSpeechRecognition();
        recognition.continuous = true;
        recognition.interimResults = true;
    
        function restartTimer() {
            timeout = setTimeout(function () {
                recognition.stop();
            }, patience * 1000);
        }
    
        recognition.onstart = function () {
            oldPlaceholder = input.placeholder;
            input.placeholder = talkMsg;
            recognizing = true;
            restartTimer();
        };
    
        recognition.onend = function () {
            recognizing = false;
            console.log("onend");
            clearTimeout(timeout);
            if (oldPlaceholder !== null)
                input.placeholder = oldPlaceholder;
            search();
        };
    
        recognition.onresult = function (event) {
            clearTimeout(timeout);
    
            // get SpeechRecognitionResultList object
            var resultList = event.results;
            console.log("onresult");
            // go through each SpeechRecognitionResult object in the list
            var finalTranscript = '';
            var interimTranscript = '';
            for (var i = event.resultIndex; i < resultList.length; ++i) {
                var result = resultList[i];
                // get this result's first SpeechRecognitionAlternative object
                var firstAlternative = result[0];
                if (result.isFinal) {
                    finalTranscript = firstAlternative.transcript;
                } else {
                    interimTranscript += firstAlternative.transcript;
                }
            }
            // capitalize transcript if start of new sentence
            var transcript = finalTranscript || interimTranscript;
    
            // append transcript to cached input value
            input.value = transcript;
    
            restartTimer();
        };
    
        record.addEventListener('click', function (event) {
            event.preventDefault();
    
            // stop and exit if already going
            if (recognizing) {
                recognition.stop();
                return;
            }

            // restart recognition
            recognition.start();
        }, false);
    });
});