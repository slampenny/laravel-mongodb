<?php

return array(
    'region'        => env('AWS_REGION'),
    'credentials'   => array(
        'key'       => env('AWS_KEY'),
        'secret'    => env('AWS_SECRET'),
        'token'     => env('AWS_TOKEN'),
    ),
);
